package database

// Database represents a key-value store, that stores all the key-value data.
// This allows insertion, querying and deleting of the key-value pairs.
type Database interface {
	// Insert allows to insert any key value pair into the database.
	//
	// The first argument is the key and the second argument is the
	// value for the key that needs to be inserted. The choice of key
	// being a byte array is so that any struct value that is converted
	// to a series of bytes can be used to as a key. The value can also
	// be any value that can fit inside the definition of an interface.
	Insert([]byte, interface{}) error
	// Query returns the most recent value for the key being queried in
	// the data base.
	Query([]byte) (interface{}, error)
	// Delete removes all the key-value pairs in the database with the given key.
	Delete([]byte) error
}
