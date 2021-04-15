package tree

import (
	"fmt"
	"reflect"
)

type AVLNodeValue struct {
	Value interface{}
}

type Operation int

const (
	GreaterThan = iota
	LesserThan
	Equal
)

func NewAVLNodeValue(val interface{}) AVLNodeValue {
	return AVLNodeValue{Value: val}
}

// compare compares the two nodes based on the operation argument.
//
// compare returns whether the "otherNode" value is "op" of the node
// on which the method is called.
// Example:
// node.Value = 1
// otherNode.Value = 2
// op = GreaterThan
// return -> 2 > 1 = true.
func (node *AVLNodeValue) compare(otherNode AVLNodeValue, op Operation) bool {


	// Check whether values are comparable.
	if reflect.TypeOf(node.Value) != reflect.TypeOf(otherNode.Value) {
		return false
	}

	typeVal := reflect.TypeOf(node.Value)
	var answer bool

	switch op {
	case GreaterThan:
		x, _ := node.Value.(typeVal)
		fmt.Println(x)
		answer = true
	case LesserThan:
	case Equal:
		answer = true
	}

	return answer
}

