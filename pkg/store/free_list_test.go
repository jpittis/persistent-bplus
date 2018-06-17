package store

import "testing"

func TestFreeList(t *testing.T) {
	f := NewFreeList(100)
	for n := 0; n < 3; n++ {
		if _, err := f.Dequeue(); err != ErrFreeListEmpty {
			t.Fatal("expected free list to be empty")
		}
		for i := 0; i < 100; i++ {
			err := f.Enqueue(i)
			if err != nil {
				t.Fatal(err)
			}
		}
		if f.Enqueue(101) != ErrFreeListFull {
			t.Fatal("expected free list to be full")
		}
		for i := 0; i < 100; i++ {
			id, err := f.Dequeue()
			if err != nil {
				t.Fatal(err)
			}
			if id != i {
				t.Fatalf("expected %d to be %d", id, i)
			}
		}
		if _, err := f.Dequeue(); err != ErrFreeListEmpty {
			t.Fatal("expected free list to be empty")
		}
	}
}
