package segment

import (
	"fmt"
	"os"
	"time"

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

// Segment describes a logical segment where the
// key-value data is stored. Each segment is
// associated with an underlying file as os.File
// which is where the data exists.
//		Each segment is also associated with an indexer.
// The indexer being located at this level has an
// advantage of not over-loading the indexer implementation
// and also enables us to operate on the segments
// for merging and compaction operations.
type Segment struct {
	// f is the handle for the underlying file
	// os.File implementation. This is where the
	// data is written in an append-only fashion.
	f *os.File
	// fName is the name of this file. It will be used
	// to open the file if it's already created but closed.
	fName string
	// idxr is the indexer decided by the user attached
	// to the particular segment. This indexer indexes
	// only the data in this particular file segment.
	idxr indexer.Indexer
	// offset holds the current offset at which the
	// last byte is written in the segment's file.
	offset int64
}

// newSegment is an internal only function used to
// create new instances of the segment object.
//
// This involves creating a new file which is the
// base of this segment and returning the segment object.
func NewSegment(idxr indexer.Indexer) (*Segment, error) {
	f, fName, err := createNewFileForSegment()
	if err != nil {
		return nil, err
	}
	return &Segment{
		f:      f,
		fName:  fName,
		idxr:   idxr,
		offset: 0,
	}, nil
}

// Append appends the given data to the given segment.
//
// After writing to the active file, it also indexes
// the object with the key in its respective indexer
// using the associated key.
func (sg *Segment) Append(key string, data string) error {

	data += defaultDelimter

	_, err := sg.f.WriteString(data)
	if err != nil {
		return err
	}

	sg.offset += int64(len(data))
	objLoc := indexer.ObjectLocation{
		Offset: sg.offset,
		Size:   len(data),
	}

	sg.idxr.Store(key, objLoc)
	return nil
}

// Query returns the data associated with the key argument
// and raises an error if it doesn't exist in this segment.
//
// The method passes on the control to the query method of
// indexer and once it returns the location of the object,
// if it exists, it reads the associated file and returns
// the object.
func (sg *Segment) Query(key string) (string, error) {
	objLoc, err := sg.idxr.Query(key)
	if err == indexer.ErrDataDoesntExistInIndexer {
		return "", err
	}

	return sg.readAt(objLoc)
}

// Print prints the associated indexer of the segment.
func (sg *Segment) Print() {
	sg.idxr.Print()
	fmt.Println("")
}

// readAt reads the data in the file associated with
// the segment using the objectLocation argument.
func (sg *Segment) readAt(objLoc indexer.ObjectLocation) (string, error) {
	b := make([]byte, objLoc.Size)

	_, err := sg.f.ReadAt(b, objLoc.Offset)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// openFileOfSegment opens the file associated with the
// segment and writes the file pointer to the segment
// object's file pointer store.
func (sg *Segment) openFileOfSegment() error {
	file, err := os.OpenFile(sg.fName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	sg.f = file
	return nil
}

// closeFileOfSegment closes the file associated
// with this segment. This is done so that the
// upper layers shouldnt get access to the file
// layers of the segment.
func (sg *Segment) closeFileOfSegment() error {
	return sg.f.Close()
}

// createNewFileForSegment creates a new file for the segment
// based on the current time as the file name and returns the
// file pointer, file name and any possible errors.
func createNewFileForSegment() (*os.File, string, error) {
	fName := time.Now().String()
	file, err := os.OpenFile(fName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, "", err
	}

	return file, fName, nil
}
