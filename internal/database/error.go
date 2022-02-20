package database

// Error is a helper type for creating constant errors.
type Error string

func (e Error) Error() string { return string(e) }

const (
	// ErrBadIndexerForEngine indicates that the indexer isn't supported for the
	// storage engine type.
	ErrBadIndexerForEngine Error = "unsupported indexer for the storage engine type"
	ErrUnsupported         Error = "unsupported feature for database"
)
