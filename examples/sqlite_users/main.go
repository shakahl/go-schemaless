package main

import (
	"context"
	"fmt"

	"github.com/dgryski/go-metro"
	"github.com/google/uuid"
	"github.com/icrowley/fake"
	"github.com/rbastic/go-schemaless"
	"github.com/rbastic/go-schemaless/core"
	st "github.com/rbastic/go-schemaless/storage/sqlite"
	"strconv"
)

const tblName = "cell"

func getShards(prefix string) []core.Shard {
	var shards []core.Shard
	nShards := 4

	for i := 0; i < nShards; i++ {
		label := prefix + strconv.Itoa(i)
		st, err := st.New(tblName, label)
		if err != nil {
			panic(err)
		}
		shards = append(shards, core.Shard{Name: label, Backend: st})
	}

	return shards
}

func hash64(b []byte) uint64 { return metro.Hash64(b, 0) }

func fakeUserJSON() string {
	name := fake.FirstName() + " " + fake.LastName()
	return "{\"name" + "\": \"" + name + "\"}"
}

func main() {
	fmt.Println("hello, multiple worlds!")

	shards := getShards("user")
	kv := schemaless.New().WithSources("user", shards).WithName("user", "user")
	defer kv.Destroy(context.TODO())

	// We're going to demonstrate jump hash+metro hash with SQLite
	// storage.  SQLite is just to make it easy to demonstrate that the data is
	// being split. You can imagine each resulting SQLite file as a separate shard.

	// As a user, you decide the refKey's purpose. For example, it can
	// be used as a record version number, or for sort-order.

	for i := 0; i < 1000; i++ {
		refKey := int64(i)
		kv.Put(context.TODO(), tblName, uuid.New().String(), "PII", refKey, fakeUserJSON())
	}
}
