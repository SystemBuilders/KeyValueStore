package storage

import (
	"context"

	"github.com/SystemBuilders/KeyValueStore/internal/indexer"
	"github.com/SystemBuilders/KeyValueStore/internal/storage/linkedlist"
	"github.com/SystemBuilders/KeyValueStore/internal/storage/segment"
)

// StorageV1 implements Storage.
//
// StorageV1 is a linked-list of "segment" objects,
// where each segment has an associated indexer.
type StorageV1 struct {
	ctx context.Context
	// fs describes the file segments.
	//
	// fs is maintained as a doubly-linked-list.
	// Each fs node has a "left" and a "right"
	// pointer which is pointing to other segments.
	// 		These segments have no chronological
	// order associated with them.
	fs *linkedlist.DLLNode
	// currSegment is the active segment where the
	// data is being written to.
	currSegment *linkedlist.DLLNode
}

var _ (Storage) = (*StorageV1)(nil)

// NewStorageV1 creates a new instance of StorageV1.
//
// This function creates a new segment, asserts
// it as the head Node of the fs linked-list and
// returns the StorageV1 object.
func NewStorageV1(ctx context.Context,
	idxrGntr indexer.IndexerGenerator,
) (*StorageV1, error) {
	segment, err := segment.NewSegment(idxrGntr.Generate())
	if err != nil {
		return nil, err
	}

	segmentNode := linkedlist.NewDLLNode(segment)
	return &StorageV1{
		ctx:         ctx,
		fs:          segmentNode,
		currSegment: segmentNode,
	}, nil
}

// Append is responsible for ensuring the data is durably
// stored inside the backing-store and can be queried
// in the future using the passed "key" argument which
// will return the "data" argument.
func (s *StorageV1) Append(key, data []byte) error {
	return s.append(string(key), string(data))
}

func (s *StorageV1) Query(key []byte) (string, error) {
	return s.query(string(key))
}

// append passes on the task of appending the data to the
// active segment and its append method.
//
// The method of segment is responsible to append the data
// the segment and index the data with the available indexer
// and make it available for querying in the future.
//
// TODO: This function will also check whether the current
// is full and create new segments for future use. This is
// located at this level rather than the segment level methods
// because the access to the segment-linked-list is available
// only at this level based on the scope.
func (s *StorageV1) append(key string, data string) error {
	curSeg := (s.currSegment.Value).(*segment.Segment)
	err := curSeg.Append(key, data)
	if err != nil {
		return err
	}

	s.print()
	// TODO: Monitor the currSegment, move to a new segment if necessary.
	return nil
}

// query is resposible for querying the storage in the
// reverse order of the active segment.
//
// If the active segment doesnt have the data, the query
// moves on to the next latest segment until the data is found.
//
// TODO: There has to be some thought around the compaction
// and merging and till where the query part continues and
// also to check whether it can go into invalid segments
// if such a thing even exists.
func (s *StorageV1) query(key string) (string, error) {
	activeSegment := s.currSegment

	var queryData string
	for {
		data, err := (activeSegment.Value).(*segment.Segment).Query(key)
		// If we see that the data doesn't exist according to the
		// segment, we move on to the previous segment if it exists.
		if err == ErrDataDoesntExistInSegment {
			if activeSegment.Left != nil {
				activeSegment = activeSegment.Left
			} else {
				return "", ErrDataNotFound
			}
		} else if err != nil {
			return "", err
		} else {
			queryData = data
			break
		}
	}

	return queryData, nil
}

func (s *StorageV1) print() {
	head := s.fs

	if head != nil {
		(head.Value).(*segment.Segment).Print()
		head = head.Right
	}
}
