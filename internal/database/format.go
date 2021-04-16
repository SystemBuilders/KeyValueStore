package database

import (
	"bytes"
	"encoding/gob"
)

func GetBytesFromInterface(val interface{}) []byte {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	encoder.Encode(val)

	return buf.Bytes()
}

func GetInterfaceFromBytes(val []byte) interface{} {
	var ret interface{}

	var buf bytes.Buffer
	dec := gob.NewDecoder(&buf)

	if err := dec.Decode(&ret); err != nil {
		panic(err)
	}

	return ret
}