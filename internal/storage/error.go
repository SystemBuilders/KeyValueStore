package storage

// Error is a helper type for creating constant errors.
type Error string

func (e Error) Error() string { return string(e) }

const (
	ErrDataNotFound Error = "the queried data does not exist in the storage"
)
