package tree

// Error is a helper type for creating constant errors.
type Error string

func (e Error) Error() string { return string(e) }

const (
	ErrNodeDoesntExist Error = "node doesn't exist in the tree"
)
