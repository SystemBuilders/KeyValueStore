package indexer

// IndexerGenerator enables creating a new instance
// of an indexer. All supported indexers should have
// an indexer generator which implements this interface.
type IndexerGenerator interface {
	// Generate creates a new instance of an Indexer.
	Generate() Indexer
}
