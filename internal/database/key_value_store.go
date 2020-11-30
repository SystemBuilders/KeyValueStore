package database

import (
	"context"
	"encoding/json"

	"github.com/SystemBuilders/KeyValueStore/internal/merge"

	"github.com/SystemBuilders/KeyValueStore/internal/file"
)

// Object describes a key-value pair in the store.
type Object struct {
	Key   interface{}
	Value interface{}
}

// ObjectLocation desribes the precise location of an Object
// in the database file.
type ObjectLocation struct {
	offset int
	size   int
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
	// index is a map indexing the key in the store to
	// the byte offset in the file.
	index map[interface{}]ObjectLocation
	// prevObjLength specifies the length of the last
	// appended object. This is used to index the next
	// log.
	prevObjLength int
}

var _ (Database) = (*KeyValueStore)(nil)

// NewKeyValueStore retunrs a new instance of a KV store.
func NewKeyValueStore(ctx context.Context) (*KeyValueStore, error) {
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
		index:         make(map[interface{}]ObjectLocation),
		prevObjLength: 0,
	}, nil
}

// Insert appends the given key and value as an Object to the file.
func (kv *KeyValueStore) Insert(key, value interface{}) error {
	obj := NewObject(key, value)
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	kv.indexLog(key, data)
	return kv.f.Append(string(data))
}

// Query returns the last appended Object type from the file, or
// the encountered error.
func (kv *KeyValueStore) Query(key interface{}) (interface{}, error) {
	logIndex := kv.index[key]
	data, err := kv.f.ReadAt(logIndex.offset, logIndex.size)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Delete deletes all entries of the
func (kv *KeyValueStore) Delete(key interface{}) error {
	return nil
}

func (kv *KeyValueStore) indexLog(key interface{}, data []byte) {
	kv.index[key] = ObjectLocation{kv.prevObjLength, len(data)}
	kv.prevObjLength += len(data)
}
