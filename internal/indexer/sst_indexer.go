package indexer

type SSTable struct {
}

var _ (Indexer) = (*SSTable)(nil)

func NewSSTableIndexer() *SSTable {
	return &SSTable{}
}

func (sst *SSTable) Store(key interface{}, loc ObjectLocation) {

}

func (sst *SSTable) Query(key interface{}) ObjectLocation {
	return ObjectLocation{}
}
