package file

import (
	"context"

	"github.com/SystemBuilders/KeyValueStore/internal/indexer"
)

// File is an interface to accessing the os.File API
// through some custome interactions, while enabling
// simpler API for the key-value store.
type File interface {
	// Append allows to append a string value to the File
	// and returns the whereabouts of the appended object
	// in appropriate fashion along with possible errors.
	Append(context.Context, string) (indexer.ObjectLocation, error)
	// ReadAt allows reading the File at the supported
	// location parameter as generated previously by this API.
	ReadAt(indexer.ObjectLocation) (string, error)
}
