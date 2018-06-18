package bplus

import (
	"io/ioutil"
	"testing"
)

func TestBPlusTree(t *testing.T) {
	_, err := newBPlusTree("b_plus_tree", 3, 20)
	if err != nil {
		t.Fatal(err)
	}
}

func newBPlusTree(filename string, branchingFactor, cacheCapacity int) (*BPlusTree, error) {
	tmpfile, err := ioutil.TempFile("", filename)
	if err != nil {
		return nil, err
	}
	tmpfile.Close()
	return NewBPlusTree(tmpfile.Name(), branchingFactor, cacheCapacity)
}
