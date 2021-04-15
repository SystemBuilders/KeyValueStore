package tree

import "fmt"

// AVLNode represents a single node of the
// AVL Tree. It has the left and right branches
// as links its successors and parent, its own value and
// the height of the node from the root of the tree,
// assuming the root is at height zero.
type AVLNode struct {
	Value AVLNodeValue
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
func newAVLNode(val AVLNodeValue) *AVLNode {
	return &AVLNode{
		Value: val,
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

	avlNodeVal := NewAVLNodeValue(val)
	if avl.headNode == nil {
		avl.headNode = newAVLNode(avlNodeVal)
		avl.headNode.Height = 0
		return nil
	}

	node := avl.headNode
	node.insert(avlNodeVal)
	return nil
}

func (avl *AVLTree) Delete(val interface{}) error {
	return nil
}

func (avl *AVLTree) Query(val interface{}) (bool,error) {
	if avl.headNode.Value.compare(NewAVLNodeValue(val),Equal) {
		return true, nil
	}
	return false, ErrNodeDoesntExist
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
		fmt.Println(node.Value)
		fmt.Println(" ")

		// Append the successors if they exist.
		if node.Left != nil {
			avl.printQueue = append(avl.printQueue, node.Left)
		}
		if node.Right != nil {
			avl.printQueue = append(avl.printQueue, node.Right)
		}
	}
}

func (node *AVLNode) insert(nodeVal AVLNodeValue) (*AVLNode, error) {
	if node == nil {
		node = newAVLNode(nodeVal)
		return node, nil
	}

	if node.Value.compare(nodeVal, GreaterThan) {
		newNode, err := node.Right.insert(nodeVal)
		if node.Right == nil {
			node.Right = newNode
			newNode.Parent = node
			return newNode, err
		}
		return node, nil
	}else {
		newNode, err := node.Left.insert(nodeVal)
		if node.Left == nil {
			node.Left = newNode
			newNode.Parent = node
			return newNode, err
		}
		return node, nil
	}
}


func (avl *AVLTree) query(val interface{}) (*AVLNode, error) {
	return nil, nil
}