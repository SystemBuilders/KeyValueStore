package indexer

// Indexer provides multiple methods to index the
// Key Value store.
type Indexer interface {
	Store(interface{}, ObjectLocation)
	Query(interface{}) ObjectLocation
}

// ObjectLocation desribes the precise location of an Object
// in the database file.
type ObjectLocation struct {
	Offset int
	Size   int

	Segment int
}
