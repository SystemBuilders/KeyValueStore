package main

import (
	"context"
	"fmt"
	"log"

	"github.com/SystemBuilders/KeyValueStore/internal/database"
)

func main() {
	ctx := context.Background()
	kv, err := database.NewKeyValueStore(ctx)
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "value")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key1", "value1")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key2", "value2")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key3", "value")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key4", "value1")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key5", "value2")
	if err != nil {
		log.Fatal(err)
	}

	data, err := kv.Query("key4")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(data)
}
