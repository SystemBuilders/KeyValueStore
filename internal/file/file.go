package file

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/SystemBuilders/KeyValueStore/internal/indexer"
)

var (
	// maxFileSize signifies the max file size accepted
	// by the key-value store.
	maxFileSize int64 = 100
	// defaultDelimiter is the delimiter set as default for
	// writing to the file.
	defaultDelimter string = "\\o/"
)

// File is an abstraction over the os.File package.
// This enables handling multiple files that may be carrying the
// data of the KV store using a single struct.
type File struct {
	fName []string
	fs    []*os.File

	// prevObjLength specifies the length of the last
	// appended object. This is used to index the next
	// log.
	prevObjLength int
	currSegment   int

	// MergeNeeded signifies whether there exists
	// more than one file segment and a merge is needed.
	MergeNeeded bool
}

// NewFile returns a new instance of File.
func NewFile(ctx context.Context, mu *sync.Mutex) (*File, error) {

	file := &File{
		fs:            []*os.File{},
		fName:         []string{},
		prevObjLength: 0,
		currSegment:   0,
		MergeNeeded:   false,
	}

	// A new WatchSet is created and set to run in
	// parallel to merge the segments of files whenever
	// necessary.
	ws := NewWatchSet(ctx, file, mu)
	go ws.RunJob()

	err := file.createNewFileSegment()
	if err != nil {
		return nil, err
	}

	return file, nil
}

// Append appends the given string to the end of the file.
//
// Append returns the precise location of the object so
// that it can be indexed - includes the offset of the data
// in the file, the size of the data and the segment of the append.
func (f *File) Append(s string) (indexer.ObjectLocation, error) {

	s += defaultDelimter
	activeSegment := f.seekToActiveFileSegment()

	info, err := os.Stat(f.fName[activeSegment])
	if err != nil {
		return indexer.ObjectLocation{}, err
	}

	// If a file segment exceeds a known limit, create a
	// new file segment, and do some book-keeping.
	if info.Size() > maxFileSize {
		fmt.Println(info.Size())
		err = f.createNewFileSegment()
		if err != nil {
			return indexer.ObjectLocation{}, err
		}
		activeSegment++

		// Magic.
		if len(f.fs) > 5 {
			f.MergeNeeded = true
		}
	}

	// Write to the active segment.
	file := f.fs[activeSegment]
	_, err = file.WriteString(s)
	if err != nil {
		return indexer.ObjectLocation{}, err
	}

	var offset int
	// If this object was appended to a new segment,
	// its offset is zero.
	if activeSegment != f.currSegment {
		offset = 0
		f.prevObjLength = 0
		f.currSegment = activeSegment
	} else {
		offset = f.prevObjLength
	}

	f.prevObjLength += len(s)

	return indexer.ObjectLocation{
		Offset:  offset,
		Size:    len(s),
		Segment: f.currSegment,
	}, nil
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
	return f.currSegment
}

// createNewFileSegment needs the parent File structure as an
// argument, creates and appends the new file-segment to the parent.
//
// This doesn't increase the activeFileIndex count.
func (f *File) createNewFileSegment() error {
	fName := time.Now().String()
	file, err := os.OpenFile(fName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	f.fs = append(f.fs, file)
	f.fName = append(f.fName, fName)

	return nil
}
