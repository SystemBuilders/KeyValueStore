package database

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"testing"

	"github.com/SystemBuilders/KeyValueStore/internal/indexer"
)

func TestAppend(t *testing.T) {
	ctx := context.Background()
	idxr := indexer.NewMapIndexer()
	kv, err := NewKeyValueStore(ctx, idxr)
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

func BenchmarkMapIndexer(b *testing.B) {
	idxr := indexer.NewMapIndexer()
	ctx := context.Background()
	kv, err := NewKeyValueStore(ctx, idxr)
	if err != nil {
		b.Fatal(err)
	}

	err = kv.Insert("key", "value")
	if err != nil {
		b.Fatal(err)
	}

	err = kv.Insert("key1", "value1")
	if err != nil {
		b.Fatal(err)
	}

	err = kv.Insert("key5", "value2")
	if err != nil {
		b.Fatal(err)
	}

	err = kv.Insert("key3", "value")
	if err != nil {
		b.Fatal(err)
	}

	err = kv.Insert("key2", "value1")
	if err != nil {
		b.Fatal(err)
	}

	err = kv.Insert("key0", "value2")
	if err != nil {
		b.Fatal(err)
	}

	_, err = kv.Query("key3")
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkSSTIndexer(b *testing.B) {
	idxr := indexer.NewSSTableIndexer()
	ctx := context.Background()
	kv, err := NewKeyValueStore(ctx, idxr)
	if err != nil {
		b.Fatal(err)
	}

	err = kv.Insert("key", "value")
	if err != nil {
		b.Fatal(err)
	}

	err = kv.Insert("key1", "value1")
	if err != nil {
		b.Fatal(err)
	}

	err = kv.Insert("key5", "value2")
	if err != nil {
		b.Fatal(err)
	}

	err = kv.Insert("key3", "value")
	if err != nil {
		b.Fatal(err)
	}

	err = kv.Insert("key2", "value1")
	if err != nil {
		b.Fatal(err)
	}

	err = kv.Insert("key0", "value2")
	if err != nil {
		b.Fatal(err)
	}

	_, err = kv.Query("key3")
	if err != nil {
		b.Fatal(err)
	}
}
