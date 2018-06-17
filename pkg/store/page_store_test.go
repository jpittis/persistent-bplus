package store

import (
	"io/ioutil"
	"testing"
)

func TestPageStoreHeaderIsFormattedCorrectly(t *testing.T) {
	store, err := newPageStore("formatted_correctly", 10)
	if err != nil {
		t.Fatal(err)
	}
	page, err := store.Load(PageID(0))
	if err != nil {
		t.Fatal(err)
	}
	expectedMagic := []byte{'E', 'K', 'A', 'J'}
	assertBufEqual(t, expectedMagic, page.Buf[0:4])
	if store.header.magicNumber != MagicNumber {
		t.Fatalf("%v != %v", store.header.magicNumber, MagicNumber)
	}

	expectedFreeList := []byte{0, 0, 0, 0}
	assertBufEqual(t, expectedFreeList, page.Buf[4:8])
	if store.header.freeList != 0 {
		t.Fatalf("%v != 0", store.header.freeList)
	}

	expectedSize := []byte{1, 0, 0, 0}
	assertBufEqual(t, expectedSize, page.Buf[8:12])
	if store.header.size != 1 {
		t.Fatalf("%v != 1", store.header.size)
	}
}

func TestPageStoreAllocatesAndDeallocatesPages(t *testing.T) {
	store, err := newPageStore("allocates_and_deallocates", 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		pageID, err := store.Allocate()
		if err != nil {
			t.Fatal(err)
		}
		if pageID != PageID(i+1) {
			t.Fatalf("expected %d == %d", pageID, i+1)
		}
	}
	if store.header.size != 11 {
		t.Fatalf("expected %d == 11", store.header.size)
	}
	for i := 0; i < 5; i++ {
		err := store.Free(PageID(i + 1))
		if err != nil {
			t.Fatal(err)
		}
	}
	if store.header.size != 11 {
		t.Fatalf("expected %d == 11", store.header.size)
	}
	for i := 0; i < 5; i++ {
		pageID, err := store.Allocate()
		if err != nil {
			t.Fatal(err)
		}
		if pageID != PageID(5-i) {
			t.Fatalf("expected %d == %d", pageID, i+1)
		}
	}
	if store.header.size != 11 {
		t.Fatalf("expected %d == 11", store.header.size)
	}
	pageID, err := store.Allocate()
	if err != nil {
		t.Fatal(err)
	}
	if pageID != PageID(11) {
		t.Fatalf("expected %d == 11", pageID)
	}
	if store.header.size != 12 {
		t.Fatalf("expected %d == 12", store.header.size)
	}
}

func newPageStore(filename string, cacheCapacity int) (*PageStore, error) {
	tmpfile, err := ioutil.TempFile("", filename)
	if err != nil {
		return nil, err
	}
	tmpfile.Close()
	return NewPageStore(tmpfile.Name(), cacheCapacity)
}

func assertBufEqual(t *testing.T, got, expected []byte) {
	if len(got) != len(expected) {
		t.Fatalf("%v != %v", got, expected)
	}
	for i := 0; i < len(got); i++ {
		if got[i] != expected[i] {
			t.Fatalf("%v != %v", got, expected)
		}
	}
}
