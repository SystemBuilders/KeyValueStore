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
	maxFileSize int64 = 3
	// defaultDelimiter is the delimiter set as default for
	// writing to the file.
	defaultDelimter string = "\\o/"
	// mergingLimit is the limit of the number of files
	// tolerable by the system and the threshold where
	// compaction and merging must occur.
	//
	// Currently an arbitrarily set number, this can be
	// based on speed, RAM size etc.
	mergingLimit int = 5
)

// FileV1 implements File.
//
// FileV1 is an abstraction over the os.File package.
// This enables handling multiple files that may be carrying the
// data of the KV store using a single struct.
type FileV1 struct {
	// fName has all the names of the files used
	// as segments.
	fName []string
	// fs stands for file segments.
	//
	// fs is the handle to the actual files
	// that hold the data.
	fs []*os.File

	// prevObjLength specifies the length of the last
	// appended object. This is used to index the next
	// log.
	prevObjLength int
	// currSegment describes which is the current working
	// segment in the "fs" data structure.
	currSegment int

	// MergeNeeded signifies whether there exists
	// more than one file segment and a merge is needed.
	//
	// A file segment describes what a single index holds
	// in the "fs" data structure. The separation into
	// file segments can be determined by any optimisations
	// needed and has no fixed rule.
	MergeNeeded bool
}

var _ (File) = (*FileV1)(nil)

// NewFileV1 returns a new instance of File.
func NewFileV1(ctx context.Context, mu *sync.Mutex, index indexer.Indexer,
) (*FileV1, error) {

	file := &FileV1{
		fName:         []string{},
		fs:            []*os.File{},
		prevObjLength: 0,
		currSegment:   0,
		MergeNeeded:   false,
	}

	// A new WatchSet is created and set to run in
	// parallel to merge the segments of files whenever
	// necessary.
	ws := NewWatchSet(ctx, file, index, mu)
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
func (f *FileV1) Append(ctx context.Context, s string,
) (indexer.ObjectLocation, error) {

	s += defaultDelimter
	activeSegment := f.seekToActiveFileSegment()

	return f.appendAtSegment(s, activeSegment)
}

// ReadAt reads the file at the given offset, segment and for the
// specified length. If there is an error, it will originate
// from a file read.
func (f *FileV1) ReadAt(loc indexer.ObjectLocation) (string, error) {
	b := make([]byte, loc.Size)

	// If the file to be read is not the active segment,
	// it must be opened before it is read.
	var file *os.File
	if loc.Segment != f.currSegment {
		var err error
		file, err = os.OpenFile(f.fName[loc.Segment], os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return "", err
		}
	} else {
		file = f.fs[loc.Segment]
	}

	_, err := file.ReadAt(b, int64(loc.Offset))
	if err != nil {
		// The new file may not contain the
		if err == io.EOF {

		}
		return "", err
	}

	// If the file that was read was not the active segment,
	// close it.
	if loc.Segment != f.currSegment {
		f.closeFileOfSegment(loc.Segment)
	}
	return string(b), nil
}

// createNewFileSegment needs the parent File structure as an
// argument, creates and appends the new file-segment to the parent.
//
// This doesn't increase the currSegment count.
func (f *FileV1) createNewFileSegment() error {
	fName := time.Now().String()
	file, err := os.OpenFile(fName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	f.fs = append(f.fs, file)
	f.fName = append(f.fName, fName)

	return nil
}

// seekToActiveFileSegment returns the current active file's
// index in the list being maintained.
func (f *FileV1) seekToActiveFileSegment() int {
	return f.currSegment
}

// appendAtSegment appends a string at a particular segment.
// This is just used by the merging functionality and is not recommended
// for external use.
func (f *FileV1) appendAtSegment(s string, segment int,
) (indexer.ObjectLocation, error) {

	info, err := os.Stat(f.fName[segment])
	if err != nil {
		return indexer.ObjectLocation{}, err
	}

	// If a file segment exceeds a known limit, create a
	// new file segment, and do some book-keeping.
	if info.Size() > maxFileSize {
		err = f.createNewFileSegment()
		if err != nil {
			return indexer.ObjectLocation{}, err
		}
		segment++

		if len(f.fs) > mergingLimit {
			f.MergeNeeded = true
		}
	}

	// Write to the active segment.
	file := f.fs[segment]
	_, err = file.WriteString(s)
	if err != nil {
		return indexer.ObjectLocation{}, err
	}

	var offset int
	// If this object was appended to a new segment,
	// its offset is zero.
	if segment != f.currSegment {
		offset = 0
		f.prevObjLength = 0
		err = f.closeActiveFile()
		if err != nil {
			fmt.Println(err)
			return indexer.ObjectLocation{}, err
		}
		f.currSegment = segment
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

// deleteFilesTillIndex deletes all files including the file at index.
//
// Sample usage -
// Initial slice: [1,2,3,4,5,6]
// Desired slice: [4,5,6]
// Function call: deleteFilesTillIndex(3)
// First 3 indexes are deleted.
func (f *FileV1) deleteFilesTillIndex(index int) {
	fmt.Println(index)
	for i := 0; i <= index; i++ {
		releaseFile(f.fName[0])
	}

	files := make([]*os.File, len(f.fs)-index-1)
	fileNames := make([]string, len(f.fName)-index-1)
	copy(files, f.fs[index:])
	copy(fileNames, f.fName[index:])

	f.fs = files
	f.fName = fileNames
}

func (f *FileV1) closeFileOfSegment(segment int) error {
	// Close the previously opened file to not encounter
	// errors of "Too many files open".
	//
	// Futher reading: https://stackoverflow.com/questions/64744802/safely-close-a-file-descriptor-in-golang
	var err error
	if _, err = os.Stat(f.fName[segment]); err == nil {
		return f.fs[segment].Close()
	}
	return err
}

func (f *FileV1) closeActiveFile() error {
	return f.closeFileOfSegment(f.currSegment)
}

func releaseFile(f string) {
	fmt.Printf("deleting: %s\n", f)
	os.Remove(f)
}
