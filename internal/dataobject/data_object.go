package dataobject

import (
	"encoding/json"
)

// Object describes a key-value pair in the store.
type Object struct {
	Key   interface{}
	Value interface{}
}

// NewObject returns a new instance of an object.
func NewObject(key, value interface{}) Object {
	return Object{
		Key:   key,
		Value: value,
	}
}

// LeastCmpFnc converts the strings as a DB object
// and returns the smaller object of the two as a
// string.
var LeastCmpFnc = func(f string, s string) string {
	var (
		first, second Object
	)

	_ = json.Unmarshal([]byte(f), &first)
	_ = json.Unmarshal([]byte(s), &second)

	if first.Key.(string) > second.Key.(string) {
		return s
	}
	return f
}
