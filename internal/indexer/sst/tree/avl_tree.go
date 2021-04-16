package tree

import (
	"bytes"
	"fmt"
	"github.com/SystemBuilders/KeyValueStore/internal/database"
)

// AVLNode represents a single node of the
// AVL Tree. It has the left and right branches
// as links its successors and parent, its own value and
// the height of the node from the root of the tree,
// assuming the root is at height zero.
type AVLNode struct {
	Value []byte
	InterfaceValue interface{}
	Left, Right, Parent *AVLNode
	Height int
}

// AVLTree implements Tree.
// AVL Tree is a self balancing binary tree.
// This can be searched similarly to a binary tree,
// all the while having a logarithmic complexity for
// the search.
type AVLTree struct {
	headNode *AVLNode

	printQueue []*AVLNode
	printItr int
}

var _ Tree = (*AVLTree)(nil)

// newAVLNode returns a new AVLNode with the
// given value and a zero height. It's left and
// right node are nil.
func newAVLNode(val interface{}) *AVLNode {

	byteData := database.GetBytesFromInterface(val)

	return &AVLNode{
		Value: byteData,
		InterfaceValue: val,
		Left: nil,
		Right: nil,
		Parent: nil,
		Height: 0,
	}
}

// NewAVLTree returns a ready to use instance
// of an AVL Tree. This always returns a nil
// head-node.
func NewAVLTree() *AVLTree {
	return &AVLTree{
		headNode: nil,
		printItr: 0,
	}
}

func (avl *AVLTree) Insert(val interface{}) error {

	if avl.headNode == nil {
		avl.headNode = newAVLNode(val)
		avl.headNode.Height = 0
		return nil
	}

	node := avl.headNode
	newNode := newAVLNode(val)
	node.insert(newNode)
	return nil
}

func (avl *AVLTree) Delete(val interface{}) error {
	return nil
}

func (avl *AVLTree) Query(val interface{}) (bool,error) {
	return true, nil
}

// Print prints the tree in a level order manner.
func (avl *AVLTree) Print() {
	avl.printQueue = append(avl.printQueue,avl.headNode)
	curLevel := 0
	for avl.printItr < len(avl.printQueue) {
		// The "dequeue" operation.
		node := avl.printQueue[avl.printItr]
		avl.printItr++

		// If the node is in the next level, print a
		// new line before printing the node.
		if node.Height > curLevel {
			fmt.Println("")
			curLevel = node.Height
		}

		// Print the node.
		fmt.Printf("%d", node.InterfaceValue)
		fmt.Printf(", ")

		// Append the successors if they exist.
		if node.Left != nil {
			avl.printQueue = append(avl.printQueue, node.Left)
		}
		if node.Right != nil {
			avl.printQueue = append(avl.printQueue, node.Right)
		}
	}
	fmt.Println("")
}

// insert has lesser control than Insert and can only insert on
// an existing tree.
//
// Inserting is a simple compare and insert mechanism that obeys
// the binary tree insertion style. This also computes the height
// of the node inserted.
// The second part of inserting is the balancing of the tree based
// on the AVL tree rules. In-depth documentation exists in the
// respective balancing functions.
func (node *AVLNode) insert(currNode *AVLNode) (*AVLNode, error) {
	if node == nil {
		node = currNode
		return node, nil
	}

	// If incoming value greater than current
	// node value.
	if bytes.Compare(node.Value, currNode.Value) < 0 {
		newNode, err := node.Right.insert(currNode)
		if node.Right == nil {
			node.Right = newNode
			newNode.Parent = node
			newNode.Height = newNode.Parent.Height + 1
			return newNode, err
		}
		return node, nil
	} else {
		newNode, err := node.Left.insert(currNode)
		if node.Left == nil {
			node.Left = newNode
			newNode.Parent = node
			newNode.Height = newNode.Parent.Height + 1
			return newNode, err
		}
		return node, nil
	}
}


func (avl *AVLTree) query(val interface{}) (*AVLNode, error) {
	return nil, nil
}