package indexer

// Error is a helper type for creating constant errors.
type Error string

func (e Error) Error() string { return string(e) }

const (
	ErrDataDoesntExistInIndexer Error = "the queried key is not indexed in the indexer"
)
