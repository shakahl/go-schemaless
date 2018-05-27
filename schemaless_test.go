package schemaless

import (
	"context"
	"github.com/rbastic/go-schemaless/core"
	"github.com/rbastic/go-schemaless/models"
	st "github.com/rbastic/go-schemaless/storage/memory"
	"strconv"
	"testing"
)

func TestShardedkv(t *testing.T) {
	var shards []core.Shard
	nElements := 1000
	nShards := 10

	for i := 0; i < nShards; i++ {
		label := "test_shard" + strconv.Itoa(i)
		// TODO(rbastic): AddShard isn't used?
		shards = append(shards, core.Shard{Name: label, Backend: st.New()})
	}

	kv := New().WithSource( shards )
	defer kv.Destroy(context.TODO())

	for i := 1; i < nElements; i++ {
		refKey := int64(i)
		kv.PutCell(context.TODO(), "test"+strconv.Itoa(i), "BASE", refKey, models.Cell{RefKey: refKey, Body: "value" + strconv.Itoa(i)})
	}

	for i := 1; i < nElements; i++ {
		k := "test" + strconv.Itoa(i)

		v, ok, err := kv.GetCellLatest(context.TODO(), k, "BASE")
		if ok != true {
			t.Errorf("failed to get key: %s\n", k)
		}
		if err != nil {
			t.Fatal(err)
		}

		if string(v.Body) != "value"+strconv.Itoa(i) {
			t.Errorf("failed to get a valid value: %v != \"value%d\"\n", v, i)
		}
	}

	for i := 1; i < nElements; i++ {
		k := "test" + strconv.Itoa(i)

		v, ok, err := kv.GetCellLatest(context.TODO(), k, "BASE")
		if err != nil {
			t.Fatal(err)
		}
		if ok != true {
			t.Errorf("failed  to get key: %s\n", k)
		}

		if string(v.Body) != "value"+strconv.Itoa(i) {
			t.Errorf("failed to get a valid value: %v != \"value%d\"\n", v, i)
		}
	}

	// and make sure we can still get to the keys
	for i := 1; i < nElements; i++ {
		k := "test" + strconv.Itoa(i)

		v, ok, err := kv.GetCellLatest(context.TODO(), k, "BASE")
		if err != nil {
			t.Fatal(err)
		}
		if ok != true {
			t.Errorf("failed to get key: %s\n", k)
		}

		if string(v.Body) != "value"+strconv.Itoa(i) {
			t.Errorf("failed to get a valid value: %v != \"value%d\"\n", v, i)
		}
	}

}
