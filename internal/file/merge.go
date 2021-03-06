package file

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/SystemBuilders/KeyValueStore/internal/indexer"

	"github.com/SystemBuilders/KeyValueStore/internal/dataobject"
)

// WatchSet is a collection of objects that a merge job
// needs to keep an eye on and use.
type WatchSet struct {
	ctx  context.Context
	f    *File
	mu   *sync.Mutex
	idxr indexer.Indexer
}

// NewWatchSet returns a new WatchSet.
func NewWatchSet(
	ctx context.Context,
	f *File,
	idxr indexer.Indexer,
	mu *sync.Mutex,
) *WatchSet {
	return &WatchSet{
		ctx:  ctx,
		f:    f,
		idxr: idxr,
		mu:   mu,
	}
}

// RunJob runs a job which checks on changes to in
// the file segments sizes. When the size reaches a
// threshold, it runs a merge job between the segments.
//
// This is supposed to run in parallel to watch on the
// WatchSet, and perform the merge job whenever deemed
// necessary.
//
// This function opens all non-operational files, at
// once (this is possible based on the estimate of the
// KV Store on how many files can stay on the memory
// safely) and use a merge-sort method to merge all the
// files in a new file, appended in a sorted manner.
//
// The simplest way to do this is to handover the operation
// to the routine Append function that is used by the
// Insert functionality. This allows us to not worry
// about updating the segment when there's an overshot
// of the file size etc. I just have a pseudo start of
// the segments portrayed to the Append function which
// takes care of the job.
//     The bad way to do this of course be to take
// control of the segments and appends, which I don't
// intend to do.
func (w *WatchSet) RunJob() {
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			if w.f.MergeNeeded {
				w.mu.Lock()
				w.f.MergeNeeded = false

				fmt.Println("Merging operation commencing")
				// Record the merging index so that we can delete the
				// files until this file index.
				mergingIndex := w.f.currSegment
				// Creating the pseudo start point for the append function.
				fmt.Println("Merging file created")
				err := w.f.createNewFileSegment()
				if err != nil {
					fmt.Println(err)
				}
				currSegment := w.f.currSegment + 1
				offsets := make([]int, len(w.f.fs))

				for {
					next, err := getNextElement(w.f, &offsets)
					if err != nil {
						break
					}
					fmt.Print(next)
					objLoc, err := w.f.appendAtSegment(next, currSegment)
					if err != nil {
					}
					w.idxr.Store(next, objLoc)
				}

				fmt.Println(mergingIndex)
				w.f.deleteFilesTillIndex(mergingIndex + 1)
				w.mu.Unlock()
			}
		}
	}
}

// getNextElement returns the next element in the merging operation
// based on the comparison function.
//
// It maintains the offset slice passed to it by updating the
// last read offset on choosing the next element to be returned.
func getNextElement(f *File, offsets *[]int) (string, error) {

	val, err := readNext(f.fs[0], (*offsets)[0])
	if err != nil {
		return "", err
	}

	least := 0
	for i := 1; i < len(f.fs); i++ {
		next, err := readNext(f.fs[i], (*offsets)[i])
		if err != nil {
			return "", err
		}
		val = dataobject.LeastCmpFnc(val, next)
		if next == val {
			least = i
		}
	}

	(*offsets)[least] += len(val) + len(defaultDelimter)
	return val, nil
}


// readNext reads the next object that was stored in the file
// after the provided offset.
//
// readNext reads the file byte-by-byte checking for the delimiter,
// which on reaching, it ends the iteration and returns the string,
func readNext(f *os.File, offset int) (string, error) {
	_, err := f.Seek(int64(offset), 0)
	if err != nil {
		return "", err
	}

	var (
		stringBuilder strings.Builder
		readBytes = 0
	)
	reader := bufio.NewReader(f)
	for {
		data, err := reader.Peek(len(defaultDelimter))
		if err != nil {
			return "", err
		}

		if string(data) == defaultDelimter {
			break
		}

		nextByte := make([]byte,1)

		_, err = reader.Read(nextByte)

		_, err = stringBuilder.Write(nextByte)
		if err != nil {
			return "", err
		}
		readBytes++
		fmt.Println(string(data))
		fmt.Println(string(data[0]))
		_, err = f.Seek(int64(offset+readBytes), 0)
		if err != nil {
			return "", err
		}
	}

	return stringBuilder.String(), nil
}