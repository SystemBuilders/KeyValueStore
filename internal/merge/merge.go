package merge

import (
	"context"

	"github.com/SystemBuilders/KeyValueStore/internal/file"
)

// WatchSet is a collection of objects that a merge job
// needs to keep an eye on and use.
type WatchSet struct {
	ctx context.Context
	f   *file.File
}

// NewWatchSet returns a new WatchSet.
func NewWatchSet(ctx context.Context, f *file.File) *WatchSet {
	return &WatchSet{
		ctx: ctx,
		f:   f,
	}
}

// RunJob runs a job which checks on changes to in
// the file segments sizes. When the size reaches a
// threshold, it runs a merge job between the segments.
//
// This is supposed to run in parallel to watch on the
// WatchSet, and perform the merge job whenever deemed
// necessary.
func (w *WatchSet) RunJob() {
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			if w.f.MergeNeeded {
				// TODO.
				// merge the two files by creating a new file.
				// Discard the old files.
				w.f.MergeNeeded = false
			}
		}
	}
}
