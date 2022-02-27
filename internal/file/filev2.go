package file

import (
	"context"
	"os"
	"time"

	"github.com/SystemBuilders/KeyValueStore/internal/file/linkedlist"
	"github.com/SystemBuilders/KeyValueStore/internal/indexer"
)

// segment describes a logical segment where the
// key-value data is stored. Each segment is
// associated with an underlying file as os.File
// which is where the data exists.
//		Each segment is also associated with an indexer.
// The indexer being located at this level has an
// advantage of not over-loading the indexer implementation
// and also enables us to operate on the segments
// for merging and compaction operations.
type segment struct {
	// f is the handle for the underlying file
	// os.File implementation. This is where the
	// data is written in an append-only fashion.
	f *os.File
	// idxr is the indexer decided by the user attached
	// to the particular segment. This indexer indexes
	// only the data in this particular file segment.
	idxr indexer.Indexer
}

// FileV2 implements File.
//
// FileV2 is a linked-list of "segment" objects,
// where each segment has an associated indexer.
type FileV2 struct {
	// fs describes the file segments.
	//
	// fs is maintained as a doubly-linked-list.
	// Each fs node has a "left" and a "right"
	// pointer which is pointing to other segments.
	// 		These segments have no chronological
	// order associated with them.
	fs *linkedlist.DLLNode
}

var _ (File) = (*FileV2)(nil)

// newSegment is an internal only function used to
// create new instances of the segment object.
//
// This involves creating a new file which is the
// base of this segment and returning the segment object.
func newSegment(idxr indexer.Indexer) (*segment, error) {
	f, err := createNewFileForSegment()
	if err != nil {
		return nil, err
	}
	return &segment{
		f:    f,
		idxr: idxr,
	}, nil
}

// NewFileV2 creates a new instance of FileV2.
//
// This function creates a new segment, asserts
// it as the head Node of the fs linked-list and
// returns the FileV2 object.
func NewFileV2(idxrGntr indexer.IndexerGenerator) (*FileV2, error) {
	segment, err := newSegment(idxrGntr.Generate())
	if err != nil {
		return nil, err
	}

	segmentNode := linkedlist.NewDLLNode(segment)
	return &FileV2{
		fs: segmentNode,
	}, nil
}

func (f *FileV2) Append(ctx context.Context, s string) (indexer.ObjectLocation, error) {
	return indexer.ObjectLocation{}, nil
}

func (f *FileV2) ReadAt(lod indexer.ObjectLocation) (string, error) {
	return "", nil
}

func (f *FileV2) Query(key []byte) {

}

func createNewFileForSegment() (*os.File, error) {
	fName := time.Now().String()
	file, err := os.OpenFile(fName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return file, nil
}
