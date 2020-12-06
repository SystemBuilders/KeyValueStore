package indexer

// Indexer provides multiple methods to index the
// Key Value store.
// An indexer can provide the most efficent way to
// index the data in the key value store.
// Multiple indexers can be maintained too.
type Indexer interface {
	Store(interface{}, ObjectLocation)
	Query(interface{}) ObjectLocation
	Print()
}

// ObjectLocation desribes the precise location of an Object
// in the database file.
type ObjectLocation struct {
	Offset int
	Size   int

	// Segment describes the segment of the file the object
	// is a part of. It is presumed that the files are always
	// segmented for a much faster and scalable approach.
	Segment int
}
