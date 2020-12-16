package file

import (
	"context"
	"os"
	"sync"
)

// WatchSet is a collection of objects that a merge job
// needs to keep an eye on and use.
type WatchSet struct {
	ctx context.Context
	f   *File
	mu  *sync.Mutex
}

// NewWatchSet returns a new WatchSet.
func NewWatchSet(
	ctx context.Context,
	f *File,
	mu *sync.Mutex,
) *WatchSet {
	return &WatchSet{
		ctx: ctx,
		f:   f,
		mu:  mu,
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
// This function opens all non-operational files,
func (w *WatchSet) RunJob() {
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			if w.f.MergeNeeded {
				// w.mu.Lock()
				// TODO.
				// merge the multiple files by creating a new file.
				// Discard the old files.
				// next, quit :=

				// _ = w.f.createNewFileSegment()
				// openFile := make([]*os.File, w.f.activeFileIndex)
				// copy(openFile, w.f.fs[:w.f.activeFileIndex])

				// w.f.MergeNeeded = false
			}
		}
	}
}

// readNext is a merging helper function that reads the next
// object in the physical file that makes up the kv store.
func readNext(f *os.File) (string, error) {
	return "", nil
}
