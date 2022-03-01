package database

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/SystemBuilders/KeyValueStore/internal/dataobject"
	"github.com/SystemBuilders/KeyValueStore/internal/indexer"
	"github.com/SystemBuilders/KeyValueStore/internal/storage"
)

// KeyValueStore implements the Database interface.
type KeyValueStore struct {
	ctx context.Context
	// s is the backing storage of the kev-value store
	// where the incoming data is persisted. This also
	// provides methods to query the stored data based
	// on the key.
	s storage.Storage
	// idxrGntr is an object which can be used to generate
	// a fresh instance of an indexer.
	//
	// This is necessary because the file layers need to
	// create new indexers per segment on the fly instead
	// of a global indexer per key value store.
	idxrGntr indexer.IndexerGenerator
	mu       *sync.Mutex
}

var _ Database = (*KeyValueStore)(nil)

// NewKeyValueStore returns a new instance of a KV store.
func NewKeyValueStore(
	ctx context.Context,
	idxrGntr indexer.IndexerGenerator,
) (*KeyValueStore, error) {

	mu := sync.Mutex{}
	s, err := storage.NewStorageV1(ctx, idxrGntr)
	if err != nil {
		return nil, err
	}

	kvStore := &KeyValueStore{
		ctx:      ctx,
		s:        s,
		idxrGntr: idxrGntr,
		mu:       &mu,
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

	return kv.insert(key, data)
}

// Query returns the last appended Object type from the file, or
// the encountered error.
// Query uses the indexed value to get the object location and
// uses the file API to query the data.
func (kv *KeyValueStore) Query(key []byte) (interface{}, error) {
	data, err := kv.s.Query(key)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Delete deletes all entries of the
func (kv *KeyValueStore) Delete(key []byte) error {
	return ErrUnsupported
}

// insert is a storage and indexer aware inserting method that
// stores the key, value pair in the storage engine and indexes
// into the appropriate indexer.
func (kv *KeyValueStore) insert(key, data []byte) error {

	switch kv.ctx.Value("storage") {
	case "append":
		// TODO: Need a wrapper function here that'll append based
		// on the ctx of the KV Store.

		err := kv.s.Append(key, data)
		if err != nil {
			return err
		}
	case "sst":
	default:
		fmt.Println(kv.ctx.Value("storage"))
		return ErrBadIndexerForEngine
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
5. Remove the files after usage, just creates a mess.
6. Fix map indexers first.
7. have a concrete thought and idea on the datatypes that we will be using. Inserting basically.
*/
