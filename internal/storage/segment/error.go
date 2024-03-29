package segment

// Error is a helper type for creating constant errors.
type Error string

func (e Error) Error() string { return string(e) }

const (
	ErrDataDoesntExistInSegment Error = "the queried key is not indexed in this segment"
)
