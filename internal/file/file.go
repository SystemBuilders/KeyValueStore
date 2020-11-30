package file

import (
	"os"
	"time"
)

// MaxFileSize signifies the max file size accepted
// by the key-value store.
var MaxFileSize int64 = 100000

// File is an abstraction over the os.File package.
// This enables handling multiple files that may be carrying the
// data of the KV store using a single struct.
type File struct {
	fName []string
	fs    []*os.File

	// MergeNeeded signifies whether there exists
	// more than one file segment and a merge is needed.
	MergeNeeded bool
}

// NewFile returns a new instance of File.
func NewFile() (*File, error) {

	file := &File{
		fs:          []*os.File{},
		fName:       []string{},
		MergeNeeded: false,
	}

	err := createNewFileSegment(file)
	if err != nil {
		return nil, err
	}

	return file, nil
}

// Append appends the given string to the end of the file.
func (f *File) Append(s string) error {
	activeFileIndex := f.seekToActiveFileSegment()

	info, err := os.Stat(f.fName[activeFileIndex])
	if err != nil {
		return err
	}

	// If a file segment exceeds a known limit, create a
	// new file segment, and do some book-keeping.
	if info.Size() > MaxFileSize {
		err = createNewFileSegment(f)
		if err != nil {
			return err
		}

		f.MergeNeeded = true
	}

	file := f.fs[activeFileIndex]
	_, err = file.WriteString(s)
	return err
}

// ReadAt reads the file at the given offset and for the
// specified length.
func (f *File) ReadAt(offset, length int) (string, error) {
	b := make([]byte, length)

	activeFileIndex := f.seekToActiveFileSegment()
	file := f.fs[activeFileIndex]
	_, err := file.ReadAt(b, int64(offset))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// seekToActiveFileSegment returns the current active file's
// index in the list being maintained.
// TODO: Figure out a way to maintain a pointer to the
// active file.
func (f *File) seekToActiveFileSegment() int {
	return 0
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
