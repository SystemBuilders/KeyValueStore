package _map

import "github.com/SystemBuilders/KeyValueStore/internal/indexer"

// MapIndexerGenerator implements IndexerGenerator.
type MapIndexerGenerator struct {
}

var _ (indexer.IndexerGenerator) = (*MapIndexerGenerator)(nil)

// NewMapIndexerGenerator creates a new instance of a
// MapIndexerGenerator.
func NewMapIndexerGenerator() *MapIndexerGenerator {
	return &MapIndexerGenerator{}
}

// Generate generates a new MpaIndexerGenerator instance.
// This instance is independent of any other existing
// indexers.
func (mig *MapIndexerGenerator) Generate() indexer.Indexer {
	return NewMapIndexer()
}
