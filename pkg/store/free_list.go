package store

import "errors"

var (
	// ErrFreeListFull is returned when the free list is at capacity.
	ErrFreeListFull = errors.New("free list full")
	// ErrFreeListEmpty is returned when the free list has no items in it.
	ErrFreeListEmpty = errors.New("free list empty")
)

// FreeList is an int circular buffer.
type FreeList struct {
	buf   []int
	front int
	back  int
	size  int
}

// NewFreeList creates a new free list of a given capacity.
func NewFreeList(capacity int) *FreeList {
	return &FreeList{
		buf:   make([]int, capacity),
		front: 0,
		back:  0,
		size:  0,
	}
}

// Dequeue removes an item off the front of the free list if one is present.
func (f *FreeList) Dequeue() (int, error) {
	if f.size == 0 {
		return 0, ErrFreeListEmpty
	}
	id := f.buf[f.front]
	f.front = (f.front + 1) % len(f.buf)
	f.size--
	return id, nil
}

// Enqueue pushes an item onto the back of the free list if there is room.
func (f *FreeList) Enqueue(id int) error {
	if f.size == len(f.buf) {
		return ErrFreeListFull
	}
	f.buf[f.back] = id
	f.back = (f.back + 1) % len(f.buf)
	f.size++
	return nil
}
