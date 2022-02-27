package file

import (
	"context"

	"github.com/SystemBuilders/KeyValueStore/internal/indexer"
)

var (
	// maxFileSize signifies the max file size accepted
	// by the key-value store.
	maxFileSize int64 = 2
	// defaultDelimiter is the delimiter set as default for
	// writing to the file.
	defaultDelimter string = "\\o/"
	// mergingLimit is the limit of the number of files
	// tolerable by the system and the threshold where
	// compaction and merging must occur.
	//
	// Currently an arbitrarily set number, this can be
	// based on speed, RAM size etc.
	mergingLimit int = 2
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
