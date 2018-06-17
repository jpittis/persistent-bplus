package bplus

import "github.com/jpittis/bplus/pkg/store"

type Key uint32

type Value []byte

type Record struct {
	Key   Key
	Value Value
}

type LeafPage struct {
	*store.Page
}

type BranchPage struct {
	*store.Page
}
