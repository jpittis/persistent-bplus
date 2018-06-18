package bplus

import (
	"encoding/binary"
	"errors"

	"github.com/jpittis/bplus/pkg/store"
)

var (
	// ErrKeyNotFound is returned when a key is not present in the tree.
	ErrKeyNotFound = errors.New("key not found")
)

// Key is the key used to lookup values in a B+ tree.
type Key uint32

// Value is the data stored in the B+ tree.
type Value []byte

// Record represents a key value pair stored in a B+ tree.
type Record struct {
	Key   Key
	Value Value
}

// Tree implemented a persisted B+ tree with a page cache.
type Tree struct {
	store           *store.PageStore
	root            *branchPage
	branchingFactor int
}

// NewTree constructs a persisted B+ tree in the given file.
func NewTree(filename string, branchingFactor, cacheCapacity int) (*Tree, error) {
	s, err := store.NewPageStore(filename, cacheCapacity)
	if err != nil {
		return nil, err
	}
	tree := &Tree{
		store:           s,
		branchingFactor: branchingFactor,
	}
	err = tree.allocateRootNode()
	return tree, err
}

func (tree *Tree) allocateRootNode() error {
	pageID, err := tree.store.Allocate()
	if err != nil {
		return err
	}
	page, err := tree.store.Load(pageID)
	if err != nil {
		return err
	}
	tree.root = &branchPage{Page: page}
	return nil
}

// Read a value from the tree, return an error if it's not found.
func (tree *Tree) Read(key Key) (Value, error) {
	if len(tree.root.keys) == 0 {
		return nil, ErrKeyNotFound
	}
	leaf, err := tree.search(key, tree.root.Page)
	if err != nil {
		return nil, err
	}
	for _, r := range leaf.records {
		if r.Key == key {
			return r.Value, nil
		}
	}
	return nil, ErrKeyNotFound
}

func (tree *Tree) search(key Key, node *store.Page) (*leafPage, error) {
	if isLeafPage(node) {
		leaf := &leafPage{Page: node}
		leaf.fromBuffer()
		return leaf, nil
	}
	branch := &branchPage{Page: node}
	branch.fromBuffer()
	var childPageID store.PageID
	if key < branch.keys[0] {
		childPageID = branch.pointers[0]
		goto foundChild
	}
	// Skip the last key.
	for i := 1; i < len(branch.keys); i++ {
		if key < branch.keys[i] {
			childPageID = branch.pointers[i]
			goto foundChild
		}
	}
	childPageID = branch.pointers[len(branch.pointers)-1]
foundChild:
	childPage, err := tree.store.Load(childPageID)
	if err != nil {
		return nil, err
	}
	return tree.search(key, childPage)
}

// Insert a key value pair into the tree. Duplicate keys are not allowed.
func (tree *Tree) Insert(key Key, value Value) error {
	return errors.New("not implemented")
}

// Delete a key value pair from the tree.
func (tree *Tree) Delete(key Key) error {
	return errors.New("not implemented")
}

type leafPage struct {
	*store.Page
	records []Record
}

func isLeafPage(page *store.Page) bool {
	return page.Buf[0] == 1
}

func (p *leafPage) toBuffer() {
	p.Buf[0] = 1
	binary.LittleEndian.PutUint32(p.Buf[1:5], uint32(len(p.records)))
	current := 5
	for _, r := range p.records {
		current += keyToBuffer(p.Buf[current:], r.Key)
		current += valueToBuffer(p.Buf[current:], r.Value)
	}
}

func keyToBuffer(buf []byte, key Key) int {
	binary.LittleEndian.PutUint32(buf[0:4], uint32(key))
	return 4
}

func valueToBuffer(buf []byte, value Value) int {
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(value)))
	for i := 0; i < len(value); i++ {
		buf[i+4] = value[i]
	}
	return 4 + len(value)
}

func (p *leafPage) fromBuffer() {
	// Skip first byte because it's the leaf page identifier.
	numRecords := binary.LittleEndian.Uint32(p.Buf[1:5])
	p.records = make([]Record, numRecords)
	current := 5
	var n int
	for i := 0; i < int(numRecords); i++ {
		p.records[i].Key, n = keyFromBuffer(p.Buf[current:])
		current += n
		p.records[i].Value, n = valueFromBuffer(p.Buf[current:])
		current += n
	}
}

func keyFromBuffer(buf []byte) (Key, int) {
	key := Key(binary.LittleEndian.Uint32(buf[0:4]))
	return key, 4
}

func valueFromBuffer(buf []byte) (Value, int) {
	valueLen := int(binary.LittleEndian.Uint32(buf[0:4]))
	value := Value(make([]byte, valueLen))
	for i := 0; i < valueLen; i++ {
		value[i] = buf[i+4]
	}
	return value, valueLen + 4
}

type branchPage struct {
	*store.Page
	keys     []Key
	pointers []store.PageID
}

func (p *branchPage) toBuffer() {
	p.Buf[0] = 0
	binary.LittleEndian.PutUint32(p.Buf[1:5], uint32(len(p.keys)))
	current := 5
	for _, key := range p.keys {
		binary.LittleEndian.PutUint32(p.Buf[current:], uint32(key))
		current += 4
	}
	binary.LittleEndian.PutUint32(p.Buf[current:], uint32(len(p.pointers)))
	current += 4
	for _, pointer := range p.pointers {
		binary.LittleEndian.PutUint32(p.Buf[current:], uint32(pointer))
		current += 4
	}
}

func (p *branchPage) fromBuffer() {
	// Skip first leaf identifier byte.
	numKeys := binary.LittleEndian.Uint32(p.Buf[1:5])
	p.keys = make([]Key, numKeys)
	current := 5
	for i := 0; i < int(numKeys); i++ {
		key := Key(binary.LittleEndian.Uint32(p.Buf[current:]))
		p.keys[i] = key
		current += 4
	}
	numPointers := binary.LittleEndian.Uint32(p.Buf[current:])
	current += 4
	p.pointers = make([]store.PageID, numPointers)
	for i := 0; i < int(numPointers); i++ {
		pointer := store.PageID(binary.LittleEndian.Uint32(p.Buf[current:]))
		p.pointers[i] = pointer
		current += 4
	}
}
