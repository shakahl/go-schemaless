package main

import (
	"context"

	"fmt"
	"github.com/dgryski/go-metro"
	"github.com/icrowley/fake"
	"github.com/rbastic/go-schemaless"
	"github.com/rbastic/go-schemaless/core"
	"github.com/rbastic/go-schemaless/models"
	st "github.com/rbastic/go-schemaless/storage/postgres"
	"github.com/satori/go.uuid"
	"os"
	"strconv"
)

func getShards(user, pass, host, port, prefix string) []core.Shard {
	var shards []core.Shard
	nShards := 4

	for i := 0; i < nShards; i++ {
		schemaName := prefix + strconv.Itoa(i)
		// TODO(rbastic): needs to map to a shard host.
		shards = append(shards, core.Shard{Name: schemaName, Backend: st.New(user, pass, host, port, schemaName)})
	}

	return shards
}

func hash64(b []byte) uint64 { return metro.Hash64(b, 0) }

func newUUID() string {
	return uuid.Must(uuid.NewV4()).String()
}

func fakeUserJSON() string {
	name := fake.FirstName() + " " + fake.LastName()
	return "{\"name" + "\": \"" + name + "\"}"
}

func main() {
	user := os.Getenv("SQLUSER")
	if user == "" {
		panic("Please specify SQLUSER=...")
	}
	pass := os.Getenv("SQLPASS")
	// TODO: SQLHOST should end up being equivalent to the computed backend label
	// For this demonstrative example, we assume you are testing all shard-schemas
	// on a single MySQL node.
	host := os.Getenv("SQLHOST")
	if host == "" {
		panic("Please specify SQLHOST=...")
	}
	port := os.Getenv("SQLPORT")
	if port == "" {
		port = "3306"
	} else {
		fmt.Println("Using port", port)
	}

	shards := getShards(user, pass, host, port, "user")
	kv := schemaless.New().WithSource(shards)
	defer kv.Destroy(context.TODO())

	// We're going to demonstrate jump hash+metro hash with MySQL-backed
	// storage. This example implements multiple shard schemas on a single
	// node.

	// You decide the refKey's purpose. For example, it can
	// be used as a record version number, or for sort-order.
	for i := 0; i < 1000; i++ {
		refKey := int64(i)
		kv.PutCell(context.TODO(), newUUID(), "PII", refKey, models.Cell{RefKey: refKey, Body: []byte(fakeUserJSON())})
	}
}
