package main

import (
	"fmt"
	"log"

	"github.com/SystemBuilders/KeyValueStore/internal/database"
)

func main() {
	kv, err := database.NewKeyValueStore()
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

	data, err := kv.Query("key1")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(data)
}
