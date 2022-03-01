package _map

import (
	"fmt"
	"sync"

	"github.com/SystemBuilders/KeyValueStore/internal/indexer"
)

// Map implements Indexer.
// The map indexer is a race-safe indexer out of the box
// and uses a single Go map to maintain the indexes of
// a key-value store.
type Map struct {
	index map[interface{}]indexer.ObjectLocation
	l     sync.Mutex
}

var _ (indexer.Indexer) = (*Map)(nil)

// NewMapIndexer returns a new map indexer.
func NewMapIndexer() *Map {
	return &Map{
		index: make(map[interface{}]indexer.ObjectLocation),
	}
}

// Type returns the type of this indexer.
func (m *Map) Type() string {
	return "map"
}

// Store indexes the key-value pair's location in
// the file using the key as the map's key and the
// object co-ordinates as the value.
//
// This is a race-safe method.
func (m *Map) Store(key interface{}, loc indexer.ObjectLocation) {
	m.l.Lock()
	keyString := key.(string)
	m.index[keyString] = loc
	m.l.Unlock()
}

// Query returns the ObjectLocation for the given key.
//
// This is a race-safe method.
func (m *Map) Query(key interface{}) (indexer.ObjectLocation, error) {
	m.l.Lock()
	if objLoc, ok := m.index[key]; ok {
		m.l.Unlock()
		return objLoc, nil
	}
	m.l.Unlock()
	return indexer.ObjectLocation{}, indexer.ErrDataDoesntExistInIndexer
}

// Print prints the indexer map.
func (m *Map) Print() {
	m.l.Lock()
	fmt.Println(m.index)
	m.l.Unlock()
}
