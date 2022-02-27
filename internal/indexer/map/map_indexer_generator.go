package _map

import "github.com/SystemBuilders/KeyValueStore/internal/indexer"

type MapIndexerGenerator struct {
}

var _ (indexer.IndexerGenerator) = (*MapIndexerGenerator)(nil)

func NewMapIndexerGenerator() *MapIndexerGenerator {
	return &MapIndexerGenerator{}
}

// Generate generates a new MpaIndexerGenerator instance.
// This instance is independent of any other existing
// indexers.
func (mig *MapIndexerGenerator) Generate() indexer.Indexer {
	return NewMapIndexer()
}
