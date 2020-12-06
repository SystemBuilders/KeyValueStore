package file

import (
	"io"
	"os"
	"time"

	"github.com/SystemBuilders/KeyValueStore/internal/indexer"
)

// MaxFileSize signifies the max file size accepted
// by the key-value store.
var MaxFileSize int64 = 10

// File is an abstraction over the os.File package.
// This enables handling multiple files that may be carrying the
// data of the KV store using a single struct.
type File struct {
	fName           []string
	fs              []*os.File
	activeFileIndex int

	// MergeNeeded signifies whether there exists
	// more than one file segment and a merge is needed.
	MergeNeeded bool
}

// NewFile returns a new instance of File.
func NewFile() (*File, error) {

	file := &File{
		fs:              []*os.File{},
		fName:           []string{},
		activeFileIndex: 0,
		MergeNeeded:     false,
	}

	err := createNewFileSegment(file)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// Append appends the given string to the end of the file.
// It returns the segment of the file that the object was
// appended to, which helps the indexer to increase precision.
func (f *File) Append(s string) (int, error) {
	activeFileIndex := f.seekToActiveFileSegment()

	info, err := os.Stat(f.fName[activeFileIndex])
	if err != nil {
		return -1, err
	}

	// If a file segment exceeds a known limit, create a
	// new file segment, and do some book-keeping.
	if info.Size() > MaxFileSize {
		err = createNewFileSegment(f)
		if err != nil {
			return -1, err
		}
		f.activeFileIndex++
		f.MergeNeeded = true
	}

	file := f.fs[f.activeFileIndex]
	_, err = file.WriteString(s)
	if err != nil {
		return -1, err
	}

	return f.activeFileIndex, err
}

// ReadAt reads the file at the given offset and for the
// specified length. If there is an error, it will originate
// from a file read.
func (f *File) ReadAt(loc indexer.ObjectLocation) (string, error) {
	b := make([]byte, loc.Size)

	file := f.fs[loc.Segment]
	_, err := file.ReadAt(b, int64(loc.Offset))
	if err != nil {
		// The new file may not contain the
		if err == io.EOF {

		}
		return "", err
	}
	return string(b), nil
}

// seekToActiveFileSegment returns the current active file's
// index in the list being maintained.
func (f *File) seekToActiveFileSegment() int {
	return f.activeFileIndex
}

// createNewFileSegment needs the parent File structure as an
// argument, creates and appends the new file-segment to the parent.
func createNewFileSegment(file *File) error {
	fName := time.Now().String()
	f, err := os.OpenFile(fName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	file.fs = append(file.fs, f)
	file.fName = append(file.fName, fName)

	return nil
}
