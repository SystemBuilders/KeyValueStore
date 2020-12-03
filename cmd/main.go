package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/SystemBuilders/KeyValueStore/internal/indexer"

	"github.com/SystemBuilders/KeyValueStore/internal/database"
)

func main() {
	ctx := context.Background()

	var (
		mapIndexerFlag = flag.Bool("map", false, "-map=true")
		sstIndexerFlag = flag.Bool("sst", false, "--sst=true")
	)

	flag.Parse()

	var idxr indexer.Indexer

	if *mapIndexerFlag {
		idxr = indexer.NewMapIndexer()
	} else if *sstIndexerFlag {
		idxr = indexer.NewSSTableIndexer()
	}

	kv, err := database.NewKeyValueStore(ctx, idxr)
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
