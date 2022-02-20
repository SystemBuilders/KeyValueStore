package database

// Database represents a key-value store, that stores all the key-value data.
// This allows insertion, querying and deleting of the key-value pairs.
type Database interface {
	// Insert allows to insert any key value pair into the database.
	Insert([]byte, interface{}) error
	// Query returns the most recent value for the key being queried in
	// the data base.
	Query([]byte) (interface{}, error)
	// Delete removes all the key-value pairs in the database with the given key.
	Delete([]byte) error
}
