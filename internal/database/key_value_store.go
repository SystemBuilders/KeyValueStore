package database

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/SystemBuilders/KeyValueStore/internal/dataobject"
	"github.com/SystemBuilders/KeyValueStore/internal/indexer"

	"github.com/SystemBuilders/KeyValueStore/internal/file"
)

// KeyValueStore implements the Database interface.
type KeyValueStore struct {
	ctx context.Context
	// f is the file where the kv store appends structured
	// logs.
	f *file.File
	// index is the way the file pertaining to the key-value
	// store is indexed. This can be used to implement
	// different indexing methods for different performance needs.
	index indexer.Indexer

	mu *sync.Mutex
}

var _ Database = (*KeyValueStore)(nil)

// NewKeyValueStore returns a new instance of a KV store.
func NewKeyValueStore(ctx context.Context, index indexer.Indexer) (*KeyValueStore, error) {

	if ctx.Value("storage") == "append" && index.Type() != "map" {
		return nil, ErrBadIndexerForEngine
	}

	if ctx.Value("storage") == "sst" && index.Type() != "sst" {
		return nil, ErrBadIndexerForEngine
	}

	mu := sync.Mutex{}
	f, err := file.NewFile(ctx, &mu, index)
	if err != nil {
		return nil, err
	}

	kvStore := &KeyValueStore{
		ctx:   ctx,
		f:     f,
		index: index,

		mu: &mu,
	}

	return kvStore, nil
}

// Insert appends the given key and value as an Object to the file.
//
// Insert writes the data to the file, gets the location of the object
// and finally indexes it into the provided indexer.
func (kv *KeyValueStore) Insert(key []byte, value interface{}) error {
	obj := dataobject.NewObject(key, value)
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	return kv.insert(key, string(data))
}

// Query returns the last appended Object type from the file, or
// the encountered error.
// Query uses the indexed value to get the object location and
// uses the file API to query the data.
func (kv *KeyValueStore) Query(key []byte) (interface{}, error) {
	kv.mu.Lock()
	logIndex := kv.index.Query(key)
	data, err := kv.f.ReadAt(logIndex)
	if err != nil {
		return nil, err
	}
	kv.mu.Unlock()
	return data, nil
}

// Delete deletes all entries of the
func (kv *KeyValueStore) Delete(key []byte) error {
	return nil
}

// indexLog indexes the log that was appended to the file.
func (kv *KeyValueStore) indexLog(key []byte, objLoc indexer.ObjectLocation) {
	kv.index.Store(
		key,
		objLoc,
	)
}

// insert is a storage and indexer aware inserting method that
// stores the key, value pair in the storage engine and indexes
// into the appropriate indexer.
func (kv *KeyValueStore) insert(key []byte, data string) error {

	switch kv.ctx.Value("storage") {
	case "append":
		// Need a wrapper function here that'll append based
		// on the ctx of the KV Store.

		objLoc, err := kv.f.Append(kv.ctx, data)
		if err != nil {
			return err
		}

		kv.indexLog(key, objLoc)
	case "sst":

	}

	return nil
}

/*
Changes that'll be made:
1. First change is the way the DB is writing. Before, there were only options of indexers. Now, there
   will be options of storage options and only the respective indexer for that storage type. For example,
   if I proceed with a append only type storage method, my indexer will be a map. Another type is the
   SSTable type storage method where I will have a SSTable indexer - a balanced tree; maybe can add options
   in the future.
2. Second change will be to the respective flows that will be taken in the storage mechanisms. The append only
   mechanism is fine but the SSTable type storage needs changes as mentioned in the future steps.
3. Third change is the SSTable storage mechanism. There needs to be a RB/AVL tree implementation done first of
   all and the following steps followed:
   • When a write comes in, add it to an in-memory balanced tree data structure (for
     example, a red-black tree). This in-memory tree is sometimes called a memtable.
   • When the memtable gets bigger than some threshold—typically a few megabytes —write it
     out to disk as an SSTable file. This can be done efficiently because the tree already
     maintains the key-value pairs sorted by key. The new SSTable file becomes the most recent
     segment of the database. While the SSTable is being written out to disk, writes can
     continue to a new memtable instance.
   • In order to serve a read request, first try to find the key in the memtable, then in
     the most recent on-disk segment, then in the next-older segment, etc.
   • From time to time, run a merging and compaction process in the background to combine
     segment files and to discard overwritten or deleted values.
4. Crash recovery - write to an append only log before inserting into the RB/AVL tree, recover later.
*/