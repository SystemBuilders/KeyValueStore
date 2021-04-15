package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/SystemBuilders/KeyValueStore/internal/indexer/map"
	"github.com/SystemBuilders/KeyValueStore/internal/indexer/sst"
	"log"
	"time"

	"github.com/SystemBuilders/KeyValueStore/internal/indexer"

	"github.com/SystemBuilders/KeyValueStore/internal/database"
)

func main() {

	var (
		appendOnlyStorageFlag = flag.Bool("appendOnlyStorage",false, "--appendOnlyStorage=true")
		sstStorageFlag = flag.Bool("sstStorage", false, "--sstStorage=true")
		mapIndexerFlag = flag.Bool("map", false, "--map=true")
		sstIndexerFlag = flag.Bool("sst", false, "--sst=true")
	)

	flag.Parse()


	ctx := context.Background()

	if *appendOnlyStorageFlag {
		ctx = context.WithValue(ctx,"storage","append")
	} else if *sstStorageFlag {
		ctx = context.WithValue(ctx,"storage","sst")
	}

	var idxr indexer.Indexer

	if *mapIndexerFlag {
		idxr = _map.NewMapIndexer()
	} else if *sstIndexerFlag {
		idxr = sst.NewSSTableIndexer()
	}

	idxr = sst.NewSSTableIndexer()

	kv, err := database.NewKeyValueStore(ctx, idxr)
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "value")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "value1")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "value2")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "value")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "value1")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "value2")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "valuenew")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "value1new")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "value2new")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "valuenew")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "value1new")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key0", "value2new")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "valuenewer")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "value1newer")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "value2newer")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "valuenewer")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "value1newer")
	if err != nil {
		log.Fatal(err)
	}

	err = kv.Insert("key", "value2newer")
	if err != nil {
		log.Fatal(err)
	}

	data, err := kv.Query("key5")
	if err != nil {
		log.Fatal(err)
	}
	// TODO: Merging same keys issue to be solved.
	time.Sleep(50000 * time.Second)
	fmt.Println(data)
}
