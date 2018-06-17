package store

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

// PageID represents the index of a page in a file. PageID multiplied with the PageSize
// produces the byte index of a page in a file.
type PageID uint32

// PageSize divides files into blocks of 4K.
const PageSize = 4096

// MagicNumber is found in the first four bytes of a page store file. (Try converting it
// to ASCII for fun!)
const MagicNumber = 0x4A414B45

// Page holds the id of a page as well as the bytes found in the file at that index.
type Page struct {
	ID  PageID
	Buf [PageSize]byte
}

// Allocator provides an interface for allocating new pages and freeing unused ones.
type Allocator interface {
	Allocate() Page
	Free(Page)
}

// PageCache provides an interface for reading, writing and caching pages in memory.
type PageCache interface {
	Load(PageID) Page
	Release(PageID)
	Write(Page)
}

var (
	// ErrPageCacheFull is returned when there is no more room in memory for a page to be
	// loaded.
	ErrPageCacheFull = errors.New("page cache full")
	// ErrPageNotFullyWritten is returned when there has been an unexpected write error.
	ErrPageNotFullyWritten = errors.New("page not fully written")
	// ErrPageNotFullyRead is returned when there has been an unexpected read error.
	ErrPageNotFullyRead = errors.New("page not fully read")
	// ErrPageNotLoaded is returned when the request page id was not found in the page
	// cache.
	ErrPageNotLoaded = errors.New("page not loaded")
)

// PageStore is a paged file store. It takes care of reading and writing pages to a given
// file, it keeps a cache of recently read pages in memory, and it provides a way to
// allocate and free new pages.
type PageStore struct {
	sync.Mutex
	file     *os.File
	cache    []Page
	lookup   map[PageID]int
	freeList *FreeList
	header   *headerPage
}

// NewPageStore is used to initialize a page store for a given file.
// If the file has yet to be used as a page store, it will be initialized.
func NewPageStore(filename string, cacheCapacity int) (*PageStore, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return nil, err
	}
	store := &PageStore{
		file:   file,
		cache:  make([]Page, cacheCapacity),
		lookup: map[PageID]int{},
	}

	// Load the header page into the first slot of the page cache.
	err = store.loadPage(PageID(0), 0)
	if err != nil {
		return nil, err
	}
	store.header = &headerPage{
		Page: &store.cache[0],
	}
	store.header.fromBuffer()
	// If the MagicNumber is not set, then we need to setup the page store.
	if store.header.magicNumber != MagicNumber {
		// Identify this file as a page store file.
		store.header.magicNumber = MagicNumber
		// A page has yet to be deallocated.
		store.header.freeList = 0
		// We're writing this header to the first page but the rest of the file is unused.
		store.header.size = 1
		store.header.toBuffer()
		err = store.Write(store.header.ID)
		if err != nil {
			return nil, err
		}
	}

	// Populate free list with the rest of the page cache slots because the cache is
	// completely empty except the first slot.
	store.freeList = NewFreeList(cacheCapacity)
	for id := 1; id < cacheCapacity; id++ {
		err := store.freeList.Enqueue(id)
		if err != nil {
			return nil, err
		}
	}

	return store, nil
}

// Load reads a page from a file into memory.
func (s *PageStore) Load(pageID PageID) (*Page, error) {
	s.Lock()
	defer s.Unlock()
	cacheID, alreadyInCache := s.lookup[pageID]
	if alreadyInCache {
		return &s.cache[cacheID], nil
	}
	cacheID, noMoreSpace := s.nextFreeCacheSlot()
	if noMoreSpace {
		return nil, ErrPageCacheFull
	}
	err := s.loadPage(pageID, cacheID)
	if err != nil {
		return nil, err
	}
	return &s.cache[pageID], nil
}

func (s *PageStore) nextFreeCacheSlot() (int, bool) {
	id, err := s.freeList.Dequeue()
	return id, err == ErrFreeListFull
}

func (s *PageStore) loadPage(pageID PageID, cacheID int) error {
	err := s.seekPageStart(pageID)
	if err != nil {
		return err
	}
	n, err := s.file.Read(s.cache[cacheID].Buf[:])
	s.cache[cacheID].ID = pageID
	s.lookup[pageID] = cacheID
	unwrittenPartOfFile := err == io.EOF
	if unwrittenPartOfFile {
		return nil
	}
	if err != nil {
		return err
	}
	if n != PageSize {
		return ErrPageNotFullyRead
	}
	return nil
}

// Release pushes a page that was previously loaded into memory out of the cache so that
// the slot can be used to load a different page.
func (s *PageStore) Release(pageID PageID) error {
	s.Lock()
	defer s.Unlock()
	cacheID, pageInCache := s.lookup[pageID]
	if !pageInCache {
		return ErrPageNotLoaded
	}
	delete(s.lookup, pageID)
	return s.releaseCacheSlot(cacheID)
}

func (s *PageStore) releaseCacheSlot(cacheID int) error {
	return s.freeList.Enqueue(cacheID)
}

// Write dumps the contents of a pages buffer to the file.
func (s *PageStore) Write(pageID PageID) error {
	s.Lock()
	defer s.Unlock()
	cacheID, pageInCache := s.lookup[pageID]
	if !pageInCache {
		return ErrPageNotLoaded
	}
	page := s.cache[cacheID]
	err := s.seekPageStart(pageID)
	if err != nil {
		return err
	}
	n, err := s.file.Write(page.Buf[:])
	if err != nil {
		return err
	}
	if n != PageSize {
		return ErrPageNotFullyWritten
	}
	return nil
}

func (s *PageStore) seekPageStart(pageID PageID) error {
	pageAddr := pageID * PageSize
	_, err := s.file.Seek(int64(pageAddr), io.SeekStart)
	return err
}

// headerPage represents the metadata schema of the first page in a page store's file.
type headerPage struct {
	*Page
	// magicNumber identifies whether the current file has been previously used as a page
	// cache.
	magicNumber uint32
	// FreeList is the start a linked list of deallocated / unused pages.
	freeList uint32
	// Size is the number of pages that the page cache has alreaedy allocated.
	size uint32
}

func (p *headerPage) fromBuffer() {
	p.magicNumber = binary.LittleEndian.Uint32(p.Buf[0:4])
	p.freeList = binary.LittleEndian.Uint32(p.Buf[4:8])
	p.size = binary.LittleEndian.Uint32(p.Buf[8:12])
}

func (p *headerPage) toBuffer() {
	binary.LittleEndian.PutUint32(p.Buf[0:4], p.magicNumber)
	binary.LittleEndian.PutUint32(p.Buf[4:8], p.freeList)
	binary.LittleEndian.PutUint32(p.Buf[8:12], p.size)
}

// Allocate and attempt to load a page from either the free list of deallocated pages or
// from the end of the file.
func (s *PageStore) Allocate() (PageID, error) {
	if s.header.freeList != 0 {
		return s.allocateFromFreeList()
	}
	return s.allocateFromEndOfFile()
}

func (s *PageStore) allocateFromFreeList() (PageID, error) {
	if s.header.freeList == 0 {
		panic("allocateFromFreeList was called with freeList == 0")
	}
	firstFreePageID := PageID(s.header.freeList / PageSize)
	page, err := s.Load(firstFreePageID)
	if err != nil {
		return 0, err
	}
	free := freePage{
		Page: page,
	}
	free.fromBuffer()
	// If we've reached the end of the free list, nextFreePage will be zero and the
	// freeList will be marked as empty.
	s.header.freeList = free.nextFreePage
	s.header.toBuffer()
	err = s.Write(s.header.ID)
	return firstFreePageID, err
}

type freePage struct {
	*Page
	nextFreePage uint32
}

func (p *freePage) fromBuffer() {
	p.nextFreePage = binary.LittleEndian.Uint32(p.Buf[0:4])
}

func (p *freePage) toBuffer() {
	binary.LittleEndian.PutUint32(p.Buf[0:4], p.nextFreePage)
}

func (s *PageStore) allocateFromEndOfFile() (PageID, error) {
	nextFreePageID := PageID(s.header.size)
	s.header.size++
	s.header.toBuffer()
	err := s.Write(s.header.ID)
	if err != nil {
		return 0, err
	}
	return nextFreePageID, nil
}

// Free places a page onto the free list so that it will be used by future allocations.
func (s *PageStore) Free(id PageID) error {
	currentFirstFreePage := s.header.freeList
	page, err := s.Load(id)
	if err != nil {
		return err
	}
	// Clear the buffer.
	for i := 0; i < PageSize; i++ {
		page.Buf[i] = 0
	}
	free := freePage{
		Page:         page,
		nextFreePage: currentFirstFreePage,
	}
	free.toBuffer()
	err = s.Write(free.ID)
	if err != nil {
		return err
	}
	s.header.freeList = uint32(id) * PageSize
	s.header.toBuffer()
	return s.Write(free.ID)
}
