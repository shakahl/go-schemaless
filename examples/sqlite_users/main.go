package main

import (
	"context"
	"fmt"

	"github.com/satori/go.uuid"
	"github.com/icrowley/fake"
	"github.com/dgryski/go-metro"
	"github.com/rbastic/go-schemaless"
	"github.com/rbastic/go-schemaless/core"
	"github.com/rbastic/go-schemaless/models"
	st "github.com/rbastic/go-schemaless/storage/fs"
	"strconv"
)

// TODO(rbastic): refactor this into a set of Strategy patterns,
// including mock patterns for tests and examples like this one.
func getShards(prefix string) []core.Shard {
	var shards []core.Shard
	nShards := 4

	for i := 0; i < nShards; i++ {
		label := prefix + strconv.Itoa(i)
		shards = append(shards, core.Shard{Name: label, Backend: st.New(label)})
	}

	return shards
}

func hash64(b []byte) uint64 { return metro.Hash64(b, 0) }

// Pregenerate these somehow, like with a channel+goroutine, if
// you need a lot of them and performance has become an issue.
func newUUID() string {
	return uuid.Must(uuid.NewV4()).String()
}

func fakeUserJSON() string {
	name := fake.FirstName() + " " + fake.LastName()
	return "{\"name" + "\": \"" + name + "\"}"
}

func main() {
	fmt.Println("hello, multiple worlds!")

	shards := getShards("user")
	kv := schemaless.New(shards)
	defer kv.Destroy(context.TODO())

	// We're going to demonstrate jump hash with FS-backed SQLite storage.
	// SQLite is just to make it easy to demonstrate that the data is being
	// split, with minimal provisioning required on the part of the user
	// (i.e. we use SQLite as it is designed to be used, for an embedded
	// scenario,)

	// You decide the refKey's purpose. For example, it can
	// be used as a record version number, or for sort-order.

	for i := 0; i < 1000; i++ {
		refKey := int64(i)
		kv.PutCell(context.TODO(), newUUID(), "PII", refKey, models.Cell{RefKey: refKey, Body: []byte(fakeUserJSON())})
	}
}
