package storage

import (
	"context"
	"fmt"
	"sync"

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
	// idxrGntr is the indexer generator which will
	// be used to pass on new indexers when creating
	// fresh segments.
	idxrGntr indexer.IndexerGenerator
	// numSegments holds the number of segments
	// currently operational with all valid keys in
	// the KV store. This will be updated by various
	// APIs inside and TODO: outside as well. Merge?
	// This will be set to 1 by default as we have
	// one segment atleast during the creation of the
	// StorageV1 object.
	numSegments int64
	// mergeNeeded is set by the storage APIs when
	// it sees the threshold of available segments
	// is breached. This sometimes might mean that
	// many unique keys exist in the KV store. Thus,
	// must be a variable number depending upon these
	// factors.
	// This will be set to false by default.
	MergeNeeded bool
	// l is the lock needed to synchronise some critical
	// variables StorageV1.
	l sync.Mutex
}

var _ (Storage) = (*StorageV1)(nil)

var (
	// mergingLimit is the limit of the number of files
	// tolerable by the system and the threshold where
	// compaction and merging must occur.
	//
	// Currently an arbitrarily set number, this can be
	// based on speed, RAM size and factors like if there's
	// actually a lot of unmergeable data in the storage.
	//
	// TODO: This can be async'ly updated by a different
	// monitoring thread.
	mergingLimit int64 = 3
)

// NewStorageV1 creates a new instance of StorageV1.
//
// This function creates a new segment, asserts
// it as the head Node of the fs linked-list and
// also creates a watch-set with the function that
// needs to be triggered during a merge and then
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
		idxrGntr:    idxrGntr,
		numSegments: 1,
		MergeNeeded: false,
		l:           sync.Mutex{},
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
// This function will also check whether the current
// is full and create new segments for future use. This is
// located at this level rather than the segment level methods
// because the access to the segment-linked-list is available
// only at this level based on the scope.
func (s *StorageV1) append(key string, data string) error {

	curSeg := (s.currSegment.Value).(*segment.Segment)

	// Monitor the currSegment, move to a new segment if necessary.
	if curSeg.IsFull {
		segment, err := segment.NewSegment(s.idxrGntr.Generate())
		if err != nil {
			return err
		}

		segmentNode := linkedlist.NewDLLNode(segment)
		s.currSegment.AppendToRight(segmentNode)
		s.currSegment = segmentNode

		s.l.Lock()
		s.numSegments++
		// If the merging limit is breached, start a
		// goroutine to handle that and move on with
		// the normal operations of the Key-Value store.
		if s.numSegments > mergingLimit {
			s.l.Unlock()
			// TODO: We might be calling this on
			// non-redundant nodes, we need to build a
			// heuristic to know if we are since it can
			// be a useless merge/compact operation.
			// Something like if the segments didn't reduce
			// post operation is a good starting point
			// to work on.
			// go s.mergeCompaction()
		} else {
			s.l.Unlock()
		}
	}

	curSeg = (s.currSegment.Value).(*segment.Segment)
	return curSeg.Append(key, data)
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
// if such a thing even exists. What we can do is, have a
// new parameter called limiter or something in the segment node.
// If this is hit, it means that the merging operation is
// taken over the nodes until here.
//     Now, we should prioritise the query over the merging
// and stop the merging/compaction operation in a stable state
// and release this new variable so that the query can finish
// its job.
func (s *StorageV1) query(key string) (string, error) {
	activeSegment := s.currSegment

	var queryData string
	for {
		data, err := (activeSegment.Value).(*segment.Segment).Query(key)
		// If we see that the data doesn't exist according to the
		// segment, we move on to the previous segment if it exists.
		if err == segment.ErrDataDoesntExistInSegment {
			// If this is the last segment, data doesn't exist
			// in the storage.
			if activeSegment.Left == nil {
				return "", ErrDataNotFound
			}
			activeSegment = activeSegment.Left
		} else if err != nil {
			return "", err
		} else {
			queryData = data
			break
		}
	}

	return queryData, nil
}

// print prints the chain of segments by calling
// the underlying segment print methods.
func (s *StorageV1) print() {
	head := s.fs

	for head != nil {
		(head.Value).(*segment.Segment).Print()
		head = head.Right
	}
	fmt.Println("")
}

// mergeCompaction enables the segments of the storage
// layer to merge and compact into non-redundant entities.
//
// This function assumes that there are atleast three nodes
// in the segment list because it ignores the current node
// and needs two more nodes atleast to perform merging
// and compaction.
//
// This function has no way to know if the segments its
// merging are already non-redundant and thus it is the
// responsibility of the user to ensure that, since this
// can be a significant user of CPU cycles.
// TODO: Figure out how?
func (s *StorageV1) mergeCompaction() {
	segmentSnapshot, currActiveSegment := s.getSegmentSnapshot()

	// TODO: Actual merging operation which should be
	// dependent on the indexer. Well sort of.

	mergedSegment := s.merge(segmentSnapshot)
	mergedSegment.Right = currActiveSegment
	currActiveSegment.Left = mergedSegment
}

// getSegmentSnapshot is responsible to provide a
// copy of the existing linkedlist of segments (except
// the active segment) without depending on the
// original list - this is because since they are
// all pointers, variable assignment won't work and
// we need to re-create the entire list.
func (s *StorageV1) getSegmentSnapshot() (
	*linkedlist.DLLNode,
	*linkedlist.DLLNode) {
	return nil, nil
}

// merge is supposed to do the actual merging of all these
// segments it has got.
func (s *StorageV1) merge(
	unmergedSegments *linkedlist.DLLNode,
) *linkedlist.DLLNode {
	return nil
}
