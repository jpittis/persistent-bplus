package bplus

import (
	"io/ioutil"
	"testing"

	"github.com/jpittis/bplus/pkg/store"
)

// Let's manually build a B+ tree that we know is in the correct format and use that to
// test our search and read functionality.
func TestBPlusTree(t *testing.T) {
	tree, err := newTree("b_plus_tree", 4, 20)
	if err != nil {
		t.Fatal(err)
	}

	// Before we do anything, let's make sure an empty tree returns an err on read rather
	// than crashing.
	value, err := tree.Read(Key(0))
	if err != ErrKeyNotFound {
		t.Fatalf("found expected value %+v", value)
	}

	// Our manual tree has 10 nodes, with a branch factor of 4 and looks something like
	// this.
	//
	//                      7
	//            /                  \
	//         3, 5                   9
	//    /      |      \         /       \
	//  1, 2 -> 3, 4 -> 5, 6 -> 7, 8, -> 9, 10

	// Let's start by allocating the 8 pages we need. (Root is already allocated.)
	for i := 0; i < 8; i++ {
		_, err = tree.store.Allocate()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Let's create our 5 leaf nodes first.
	leaf12Page, err := tree.store.Load(store.PageID(2))
	if err != nil {
		t.Fatal(err)
	}
	leaf12 := &leafPage{Page: leaf12Page}
	leaf12.records = []Record{
		{Key: 1, Value: []byte{1}},
		{Key: 2, Value: []byte{2}},
	}
	leaf12.toBuffer()

	leaf34Page, err := tree.store.Load(store.PageID(3))
	if err != nil {
		t.Fatal(err)
	}
	leaf34 := &leafPage{Page: leaf34Page}
	leaf34.records = []Record{
		{Key: 3, Value: []byte{3}},
		{Key: 4, Value: []byte{4}},
	}
	leaf34.toBuffer()

	leaf56Page, err := tree.store.Load(store.PageID(4))
	if err != nil {
		t.Fatal(err)
	}
	leaf56 := &leafPage{Page: leaf56Page}
	leaf56.records = []Record{
		{Key: 5, Value: []byte{5}},
		{Key: 6, Value: []byte{6}},
	}
	leaf56.toBuffer()

	leaf78Page, err := tree.store.Load(store.PageID(5))
	if err != nil {
		t.Fatal(err)
	}
	leaf78 := &leafPage{Page: leaf78Page}
	leaf78.records = []Record{
		{Key: 7, Value: []byte{7}},
		{Key: 8, Value: []byte{8}},
	}
	leaf78.toBuffer()

	leaf910Page, err := tree.store.Load(store.PageID(6))
	if err != nil {
		t.Fatal(err)
	}
	leaf910 := &leafPage{Page: leaf910Page}
	leaf910.records = []Record{
		{Key: 9, Value: []byte{9}},
		{Key: 10, Value: []byte{10}},
	}
	leaf910.toBuffer()

	// Then our 2 branch nodes.
	branch35Page, err := tree.store.Load(store.PageID(7))
	if err != nil {
		t.Fatal(err)
	}
	branch35 := &branchPage{Page: branch35Page}
	branch35.keys = []Key{3, 5}
	branch35.pointers = []store.PageID{2, 3, 4}
	branch35.toBuffer()

	branch9Page, err := tree.store.Load(store.PageID(8))
	if err != nil {
		t.Fatal(err)
	}
	branch9 := &branchPage{Page: branch9Page}
	branch9.keys = []Key{9}
	branch9.pointers = []store.PageID{5, 6}
	branch9.toBuffer()

	// And finally the root node.
	root := tree.root
	root.keys = []Key{7}
	root.pointers = []store.PageID{7, 8}
	root.toBuffer()

	// Search for all the keys and make sure they're found.
	for key := 1; key < 11; key++ {
		value, err := tree.Read(Key(key))
		if err != nil {
			t.Fatal(key, err)
		}
		if int(value[0]) != key {
			t.Fatalf("expected %d == %d", value[0], key)
		}
	}
	// Test that we can't find some keys that shoudn't be in the tree.
	value, err = tree.Read(Key(0))
	if err != ErrKeyNotFound {
		t.Fatalf("found expected value %+v", value)
	}
	value, err = tree.Read(Key(11))
	if err != ErrKeyNotFound {
		t.Fatalf("found expected value %+v", value)
	}
}

func newTree(filename string, branchingFactor, cacheCapacity int) (*Tree, error) {
	tmpfile, err := ioutil.TempFile("", filename)
	if err != nil {
		return nil, err
	}
	tmpfile.Close()
	return NewTree(tmpfile.Name(), branchingFactor, cacheCapacity)
}
