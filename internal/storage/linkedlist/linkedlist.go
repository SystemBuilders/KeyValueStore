package linkedlist

import "fmt"

// DLLNode is a node of a non circular doubly
// linked list.
//
// Example:
// {n1} <-> {n2} <-> {n3} <-> {n4} -> nil
type DLLNode struct {
	Left  *DLLNode
	Right *DLLNode
	Value interface{}
}

func NewDLLNode(value interface{}) *DLLNode {
	return &DLLNode{
		Left:  nil,
		Right: nil,
		Value: value,
	}
}

// AppendToRight appends the given node to the right
// of the current node (node on which the function
// is called).
func (dll *DLLNode) AppendToRight(node *DLLNode) {
	if dll.Right == nil {
		dll.Right = node
		node.Left = dll
		return
	}

	rightNode := dll.Right
	dll.Right = node
	node.Left = dll

	node.Right = rightNode
	rightNode.Left = node
}

// AppendToLeft appends the given node to the left
// of the current node (node on which the function
// is called).
func (dll *DLLNode) AppendToLeft(node *DLLNode) {
	if dll.Left == nil {
		dll.Left = node
		node.Right = dll
		return
	}

	leftNode := dll.Left
	dll.Left = node
	node.Right = dll

	node.Left = leftNode
	leftNode.Right = node
}

// Print prints the values of the nodes from the
// node on which the function is being called.
func (dll *DLLNode) Print() {
	tempNode := dll
	for tempNode != nil {
		fmt.Print(tempNode.Value)
		fmt.Printf(" <-> ")
		tempNode = tempNode.Right
	}

	fmt.Println("")
}
