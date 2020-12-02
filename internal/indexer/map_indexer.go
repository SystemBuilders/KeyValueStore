package indexer

// Map implements Storage.
type Map struct {
	index map[interface{}]ObjectLocation
}

var _ (Indexer) = (*Map)(nil)

// NewMapIndexer returns a new map indexer.
func NewMapIndexer() *Map {
	return &Map{
		make(map[interface{}]ObjectLocation),
	}
}

// Store indexes the key-value pair's location in
// the file using the key as the map's key and the
// object co-ordinates as the value.
func (m *Map) Store(key interface{}, loc ObjectLocation) {
	m.index[key] = loc
}

// Query returns the
func (m *Map) Query(key interface{}) ObjectLocation {
	return m.index[key]
}
