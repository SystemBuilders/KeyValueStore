package indexer

// Indexer provides multiple methods to index the
// Key Value store.
// An indexer can provide the most efficent way to
// index the data in the key value store.
// Multiple indexers can be maintained too.
type Indexer interface {
	// Type returns the type of the indexer.
	Type() string
	// Store lets the user to index a particular key
	// with the given ObjectLocation in the indexer.
	Store(interface{}, ObjectLocation)
	// Query allows the user to query the indexer.
	// Based on the QueryType parameter, data can be
	// queried in multiple ways.
	Query(interface{}) (ObjectLocation, error)
	// Print prints the indexer in an explicit manner.
	Print()
}

// ObjectLocation describes the precise location of an Object
// in the database file.
type ObjectLocation struct {
	// Offset describes the starting position of this
	// object from the beginning of the file. This is
	// the number that is calculated by the APIs written
	// here and not from any external APIs.
	Offset int64
	// Size describes the size of this particular object.
	Size int
}

// QueryType allows to query the indexer in a desired manner.
type QueryType int

// Describes the different Query types.
const (
	QueryRandom QueryType = iota
	QueryLeast
	QueryHighest
)
