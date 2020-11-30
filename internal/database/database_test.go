package database

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"testing"
)

func TestAppend(t *testing.T) {
	ctx := context.Background()
	kv, err := NewKeyValueStore(ctx)
	if err != nil {
		log.Fatal(err)
	}

	insertCount := 0
	for {
		if insertCount == 1000 {
			break
		}

		value := "value" + strconv.Itoa(insertCount)
		err = kv.Insert("key", value)
		if err != nil {
			log.Fatal(err)
		}
		insertCount++
	}

	data, err := kv.Query("key")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(data)
}
