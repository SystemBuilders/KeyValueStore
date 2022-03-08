package segment

import (
	"io/ioutil"
	"testing"

	_map "github.com/SystemBuilders/KeyValueStore/internal/indexer/map"
	"github.com/stretchr/testify/assert"
)

// Test_AppendAndQuery ensures that the data being
// written to the segment is what is being given
// out on a query to the segment.
func Test_AppendAndQuery(t *testing.T) {

	idxr := _map.NewMapIndexer()
	sg, err := NewSegment(idxr)
	assert.Nil(t, err)

	testKey := "keyString"
	testData := "dataString"

	err = sg.Append(testKey, testData)
	assert.Nil(t, err)

	obtainedData, err := sg.Query(testKey)
	assert.Nil(t, err)

	assert.Equal(t, testData, obtainedData)
}

// Test_Append tests whether the data that was written
// into the file is what is expected as being written.
//
// This means that it involes just the Append function
// and then reading the file using raw file functions
// and checking the data is the same.
//
// TODO: Can check all file offsets etc.
func Test_Append(t *testing.T) {
	idxr := _map.NewMapIndexer()
	sg, err := NewSegment(idxr)
	assert.Nil(t, err)

	testKey := "keyString"
	testData := "dataString"

	err = sg.Append(testKey, testData)
	assert.Nil(t, err)

	fileData, err := ioutil.ReadFile(sg.fName)
	assert.Nil(t, err)

	writtenData := testData + defaultDelimter

	assert.Equal(t, writtenData, string(fileData))
}

func Test_Query(t *testing.T) {
	idxr := _map.NewMapIndexer()
	sg, err := NewSegment(idxr)
	assert.Nil(t, err)

	testKey := "keyString"
	testData := "dataString"

	err = sg.Append(testKey, testData)
	assert.Nil(t, err)

	// TODO:
}

// Test_verifyFileSizeLimits creates a file which is
// above the limit using methods other than Append
// and check whether the "IsFull" parameter is set to
// true.
// It also tests when smaller data is written, the
// function acts accordingly.
func Test_verifyFileSizeLimits(t *testing.T) {
	idxr := _map.NewMapIndexer()
	
	// Testing true case.
	sg, err := NewSegment(idxr)
	assert.Nil(t, err)

	testData := "dataStringJustExtendingTheSpaceNow"
	_, err = sg.f.WriteString(testData)
	assert.Nil(t, err)

	assert.False(t, sg.IsFull)
	err = sg.verifyFileSizeLimits(10)
	assert.Nil(t, err)
	assert.True(t, sg.IsFull)

	// Testing false case.
	testData = "smolData"
	sg2, err := NewSegment(idxr)
	assert.Nil(t, err)
	_, err = sg2.f.WriteString(testData)
	assert.Nil(t, err)
	assert.False(t, sg2.IsFull)

	err = sg2.verifyFileSizeLimits(10)
	assert.Nil(t,err)
	assert.False(t,sg2.IsFull)
}

// Test_readAt tests whether the right string is
// returned based on the location provided as the
// arguments.
//
// This is done by appending some data and then
// accessing the indexer of the segment which will
// used to get the location of the object.
func Test_readAt(t *testing.T) {
	idxr := _map.NewMapIndexer()
	sg, err := NewSegment(idxr)
	assert.Nil(t, err)

	testKey := "keyString"
	testData := "dataString"

	err = sg.Append(testKey, testData)
	assert.Nil(t,err)
	

}