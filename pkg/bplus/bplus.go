package bplus

import (
	"encoding/binary"

	"github.com/jpittis/bplus/pkg/store"
)

type Key uint32

type Value []byte

type Record struct {
	Key   Key
	Value Value
}

type BPlusTree struct {
	store           *store.PageStore
	root            *branchPage
	branchingFactor int
}

func NewBPlusTree(filename string, branchingFactor, cacheCapacity int) (*BPlusTree, error) {
	s, err := store.NewPageStore(filename, cacheCapacity)
	if err != nil {
		return nil, err
	}
	tree := &BPlusTree{
		store:           s,
		branchingFactor: branchingFactor,
	}
	err = tree.allocateRootNode()
	return tree, err
}

func (tree *BPlusTree) allocateRootNode() error {
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

type leafPage struct {
	*store.Page
	records []Record
}

func isLeafPage(page store.Page) bool {
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
	total := 4 + len(value)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(value)))
	for i := 4; i < total; i++ {
		buf[i] = value[i-4]
	}
	return total
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
	total := 4 + valueLen
	for i := 4; i < total; i++ {
		value[i] = buf[i]
	}
	return value, valueLen
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
	for _, pointer := range p.pointers {
		binary.LittleEndian.PutUint32(p.Buf[current:], uint32(pointer))
		current += 4
	}
}

func (p *branchPage) fromBuffer() {
	numKeys := binary.LittleEndian.Uint32(p.Buf[1:5])
	p.keys = make([]Key, numKeys)
	current := 5
	for i := 0; i < int(numKeys); i++ {
		key := Key(binary.LittleEndian.Uint32(p.Buf[current:]))
		p.keys[i] = key
		current += 4
	}
	numPointers := binary.LittleEndian.Uint32(p.Buf[current:])
	p.pointers = make([]store.PageID, numPointers)
	for i := 0; i < int(numPointers); i++ {
		pointer := store.PageID(binary.LittleEndian.Uint32(p.Buf[current:]))
		p.pointers[i] = pointer
		current += 4
	}
}
