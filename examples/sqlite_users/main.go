package main

import (
	"context"
	"fmt"

	"github.com/dgryski/go-metro"
	"github.com/icrowley/fake"
	"github.com/rbastic/go-schemaless"
	"github.com/rbastic/go-schemaless/core"
	"github.com/rbastic/go-schemaless/models"
	st "github.com/rbastic/go-schemaless/storage/fs"
	"github.com/satori/go.uuid"
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
	kv := schemaless.New().WithSource(shards)
	defer kv.Destroy(context.TODO())

	// We're going to demonstrate jump hash+metro hash with FS-backed SQLite
	// storage.  SQLite is just to make it easy to demonstrate that the data is
	// being split, with minimal provisioning required on the part of the user
	// (i.e. we use SQLite as it is designed to be used, for an embedded
	// scenario.) You can imagine each SQLite file as a separate shard.

	// You decide the refKey's purpose. For example, it can
	// be used as a record version number, or for sort-order.

	for i := 0; i < 1000; i++ {
		refKey := int64(i)
		kv.PutCell(context.TODO(), newUUID(), "PII", refKey, models.Cell{RefKey: refKey, Body: []byte(fakeUserJSON())})
	}
}
