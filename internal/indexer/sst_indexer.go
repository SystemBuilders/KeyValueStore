package indexer

// SSTable stands for Sorted Segment Table.
// An SSTable is a collection of a number of file
// segments where each segment has the objects
// in sorted order by the key of the object.
type SSTable struct {
	list []SSTableObject
}

// SSTableObject is the complex struct of a key
// to be indexed and the object location in the segment.
type SSTableObject struct {
	key interface{}
	loc ObjectLocation
}

var _ (Indexer) = (*SSTable)(nil)

// NewSSTableIndexer creates a new SSTable indexer.
func NewSSTableIndexer() *SSTable {
	return &SSTable{}
}

// Store inserts into the sorted list of objects.
func (sst *SSTable) Store(key interface{}, loc ObjectLocation) {

}

// Query is a simple binary search over the sorted list of
// objects in the SSTable.
func (sst *SSTable) Query(key interface{}) ObjectLocation {
	return ObjectLocation{}
}
