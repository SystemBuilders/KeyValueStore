package linkedlist

import (
	"testing"
)

func TestAll(t *testing.T) {
	head := NewDLLNode(0)

	firstNode := NewDLLNode(1)
	head.AppendToRight(firstNode)

	thirdNode := NewDLLNode(3)
	firstNode.AppendToRight(thirdNode)

	secondNode := NewDLLNode(2)
	thirdNode.AppendToLeft(secondNode)

	head.Print()
}
