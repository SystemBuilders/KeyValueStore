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
func (kv *KeyValueStore) Insert(key, value interface{}) error {
	kv.mu.Lock()
	obj := dataobject.NewObject(key, value)
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	objLoc, err := kv.f.Append(string(data))
	if err != nil {
		return err
	}

	kv.indexLog(key, objLoc)
	kv.mu.Unlock()
	return nil
}

// Query returns the last appended Object type from the file, or
// the encountered error.
// Query uses the indexed value to get the object location and
// uses the file API to query the data.
func (kv *KeyValueStore) Query(key interface{}) (interface{}, error) {
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
func (kv *KeyValueStore) Delete(key interface{}) error {
	return nil
}

// indexLog indexes the log that was appended to the file.
func (kv *KeyValueStore) indexLog(key interface{}, objLoc indexer.ObjectLocation) {
	kv.index.Store(
		key,
		objLoc,
	)
}
