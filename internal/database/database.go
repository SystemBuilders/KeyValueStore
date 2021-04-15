package database

// Database represents a kev-value store, that stores all the key-value data.
// This allows insertion, querying and deleting of the key-value pairs.
type Database interface {
	// Insert allows to insert any key value pair into the database.
	Insert(Item, interface{}) error
	// Query returns the most recent value for the key being queried in
	// the data base.
	Query(Item) (interface{}, error)
	// Delete removes all the key-value pairs in the database with the given key.
	Delete(Item) error
}

// Item describes a key in database.
// All keys must implement this interface in order
// to be processed.
type Item interface {
	Less(Item) bool
}
