package tree

// Tree represents a tree object that is stored in the RAM
// and can be used as an indexer for fast data retrieval.
// It allows, insertion, deletion and querying of data
// out of the box and all tree's must implement these methods
// to belong to the Tree interface type.
type Tree interface {
	// Insert inserts the data into the tree.
	// It takes in an interface and returns an error
	// on whether the operation failed or not.
	// A nil error means that the operation was fine.
	Insert(interface{}) error
	// Delete deletes the data from the tree.
	// It takes in the value to be deleted and returns
	// an error if the deletion didn't go through,
	// including if the node doesn't exist in the tree.
	Delete(interface{}) error
	// Query queries the tree for the data.
	// True is returned if the object exists in the tree.
	// errors and false if it doesn't.
	Query(interface{}) (bool, error)
	// Print prints the tree in a human readable manner.
	Print()
}
