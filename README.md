A persistent B+ tree and page cache built for fun and learning.

- `pkg/store/page_store.go` is a buffer cache / page cache which takes care of loading
  files, reading and writing pages from disk as well as allocating and freeing pages on
  disk.

- `pkg/store/bplus.go` has the ability to search a persisted B+ tree but has yet to have
  insert and delete implemented.
