package database

import (
	"context"
	"encoding/json"

	"github.com/SystemBuilders/KeyValueStore/internal/indexer"
	"github.com/SystemBuilders/KeyValueStore/internal/merge"

	"github.com/SystemBuilders/KeyValueStore/internal/file"
)

// Object describes a key-value pair in the store.
type Object struct {
	Key   interface{}
	Value interface{}
}

// NewObject returns a new instance of an object.
func NewObject(key, value interface{}) Object {
	return Object{
		Key:   key,
		Value: value,
	}
}

// KeyValueStore implements the Database interface.
type KeyValueStore struct {
	ctx context.Context
	// f is the file where the kv store appends structured
	// logs.
	f *file.File
	// index is the way the file pertaining to the key-value
	// store is indexed. This can be used to implement
	// differet indexing methods for different performance needs.
	index indexer.Indexer
	// prevObjLength specifies the length of the last
	// appended object. This is used to index the next
	// log.
	prevObjLength int
	currSegment   int
}

var _ (Database) = (*KeyValueStore)(nil)

// NewKeyValueStore retunrs a new instance of a KV store.
func NewKeyValueStore(ctx context.Context, index indexer.Indexer) (*KeyValueStore, error) {
	f, err := file.NewFile()
	if err != nil {
		return nil, err
	}

	// A new WatchSet is created and set to run in
	// parallel to merge the segments of files whenever
	// necessary.
	ws := merge.NewWatchSet(ctx, f)
	go ws.RunJob()

	return &KeyValueStore{
		ctx:           ctx,
		f:             f,
		index:         index,
		prevObjLength: 0,
		currSegment:   -1,
	}, nil
}

// Insert appends the given key and value as an Object to the file.
func (kv *KeyValueStore) Insert(key, value interface{}) error {
	obj := NewObject(key, value)
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	segment, err := kv.f.Append(string(data))
	if err != nil {
		return err
	}

	if kv.currSegment == -1 {
		kv.currSegment = segment
	}

	kv.indexLog(key, data, segment)
	return nil
}

// Query returns the last appended Object type from the file, or
// the encountered error.
func (kv *KeyValueStore) Query(key interface{}) (interface{}, error) {
	logIndex := kv.index.Query(key)
	data, err := kv.f.ReadAt(logIndex)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Delete deletes all entries of the
func (kv *KeyValueStore) Delete(key interface{}) error {
	return nil
}

// indexLog indexes the log that was appended to the file.
func (kv *KeyValueStore) indexLog(key interface{}, data []byte, segment int) {
	var offset int
	// If this object was appended to a new segment,
	// its offset is zero.
	if kv.currSegment != segment {
		offset = 0
		kv.prevObjLength = 0
		kv.currSegment = segment
	} else {
		offset = kv.prevObjLength
	}

	kv.index.Store(
		key,
		indexer.ObjectLocation{
			Offset:  offset,
			Size:    len(data),
			Segment: segment,
		},
	)

	kv.prevObjLength += len(data)
}
