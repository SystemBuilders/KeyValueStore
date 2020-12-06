package indexer

import "fmt"

// SSTable stands for Sorted Segment Table.
// An SSTable is a collection of a number of file
// segments where each segment has the objects
// in sorted order by the key of the object.
type SSTable struct {
	list        [][]SSTableObject
	currSegment int
}

// SSTableObject is the complex struct of a key
// to be indexed and the object location in the segment.
type SSTableObject struct {
	key interface{}
	loc ObjectLocation
}

var _ (Indexer) = (*SSTable)(nil)

// NewSSTableIndexer creates a new SSTable indexer.
func NewSSTableIndexer() *SSTable {
	return &SSTable{
		currSegment: -1,
	}
}

// Store inserts into the sorted list of objects.
//
// Iterate over the list until the first object bigger than the
// inserting element is found and insert at that position.
func (sst *SSTable) Store(key interface{}, loc ObjectLocation) {
	sstObject := SSTableObject{key, loc}
	if sst.currSegment < loc.Segment {
		var segmentList []SSTableObject
		sst.list = append(sst.list, segmentList)
		sst.currSegment++
	}

	if len(sst.list[sst.currSegment]) == 0 {
		sst.list[sst.currSegment] = append(sst.list[sst.currSegment], sstObject)
		return
	}
	if len(sst.list[sst.currSegment]) == 1 {
		if sst.list[sst.currSegment][0].key.(string) > key.(string) {
			sst.list[sst.currSegment] = append(
				[]SSTableObject{sstObject}, sst.list[sst.currSegment]...,
			)
		} else {
			sst.list[sst.currSegment] = append(sst.list[sst.currSegment], sstObject)
		}
		return
	}

	for i, v := range sst.list[sst.currSegment] {
		if key.(string) < v.key.(string) {
			if i == 0 {
				sst.list[sst.currSegment] = append([]SSTableObject{sstObject}, sst.list[sst.currSegment]...)
				break
			}
			sst.list[sst.currSegment] = insertAt(i, SSTableObject{key, loc}, sst.list[sst.currSegment])
			break
		}
	}
	if key.(string) >= sst.list[sst.currSegment][len(sst.list[sst.currSegment])-1].key.(string) {
		sst.list[sst.currSegment] = append(sst.list[sst.currSegment], SSTableObject{key, loc})
	}
}

// Query is a simple binary search over the sorted list of
// objects in the SSTable.
// It searches backwards in the segments to find the most recently
// appended value in the store.
func (sst *SSTable) Query(key interface{}) ObjectLocation {
	currSegment := sst.currSegment
	for {
		val, ok := binarySearch(sst.list[currSegment], key)
		if !ok {
			if currSegment != 0 {
				currSegment--
			} else {
				// Object not found.
				return ObjectLocation{}
			}
		} else {
			return val
		}
	}
}

// Print prints the SSTable.
func (sst *SSTable) Print() {
	fmt.Println(sst.list)
}

// insertAt inserts at the provided index of the list.
// insertAt is 0 - indexed.
//
// Example:
// Initial slice: a = [1,2,3,4,5,6,7]
// insertAt(3,9,a) returns [1,2,3,9,4,5,6,7]
//
// insertAt DOES NOT CHECK FOR END OF LIST, PLEASE TAKE
// CARE TO PASS APPROPRIATE ARGUMENTS.
func insertAt(i int, key SSTableObject, list []SSTableObject) []SSTableObject {
	rightSlice := list[i:]
	copyOfRightSlice := make([]SSTableObject, len(rightSlice))
	copy(copyOfRightSlice, rightSlice)
	list = append(list[:i], key)
	list = append(list[:i+1], copyOfRightSlice...)
	return list
}

// binarySearch does a simple binary search of the key in
// the list. The second argument returns false if the object
// was not found in the list.
func binarySearch(list []SSTableObject, key interface{}) (ObjectLocation, bool) {
	lenList := len(list)
	if lenList == 0 {
		return ObjectLocation{}, false
	}
	if lenList == 1 {
		if key.(string) != list[0].key.(string) {
			return ObjectLocation{}, false
		}
		return list[0].loc, true
	}

	low := 0
	high := lenList - 1
	for high >= low {
		pivot := low + (high-low)/2

		if list[pivot].key.(string) == key.(string) {
			return list[pivot].loc, true
		}

		if list[pivot].key.(string) < key.(string) {
			low = pivot + 1
		} else {
			high = pivot - 1
		}
	}
	return ObjectLocation{}, false
}
