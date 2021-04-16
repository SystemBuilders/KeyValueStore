package tree

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAVLTree_Insert(t *testing.T) {
	avlTree := NewAVLTree()

	assert := assert.New(t)
	err := avlTree.Insert(1)
	assert.Nil(err)

	ok, err := avlTree.Query(1)
	assert.True(ok)
	assert.Nil(err)

	err = avlTree.Insert(2)
	assert.Nil(err)

	err = avlTree.Insert(3)
	assert.Nil(err)

	err = avlTree.Insert(-1)
	assert.Nil(err)

	err = avlTree.Insert(-2)
	assert.Nil(err)

	err = avlTree.Insert(4)
	assert.Nil(err)

	err = avlTree.Insert(0)
	assert.Nil(err)

	avlTree.Print()
}
