package storage

// Storage describes the persistent storage
// that stores the data from the key-value store
// in a structured manner.
//
// 		This must be able to perform two operations,
// Append and Query of the data being fed into
// the key-value store.
//
// 		The implementation of Storage can take
// multiple versions and the underlying objects
// or handles can change as long as they can perform
// these functions.
type Storage interface {
	// Append allows the key-value store to append
	// incoming data in the form of a stream of bytes.
	//
	// The first argument is the key, which the storage
	// needs as it is in charge of indexing the stored
	// value in the desired indexer as well.
	Append([]byte, []byte) error
	// Query allows the key-value store to get
	// back the data that was stored from the
	// provided key value as argument.
	Query([]byte) (string, error)
}
