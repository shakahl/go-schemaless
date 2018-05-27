package core

import (
	"context"
	"strconv"
	"testing"

	ch "github.com/dgryski/go-shardedkv/choosers/chash"
	"github.com/rbastic/go-schemaless/models"
	st "github.com/rbastic/go-schemaless/storage/memory"
)

func TestShardedkv(t *testing.T) {
	var shards []Shard
	nElements := 1000
	nShards := 10

	for i := 0; i < nShards; i++ {
		label := "test_shard" + strconv.Itoa(i)
		// TODO(rbastic): AddShard isn't used here?
		shards = append(shards, Shard{Name: label, Backend: st.New()})
	}

	chooser := ch.New()

	kv := New(chooser, shards)
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

	var migrationBuckets []string

	for i := nShards; i < nShards*2; i++ {
		label := "test_shard" + strconv.Itoa(i)
		migrationBuckets = append(migrationBuckets, label)
		backend := st.New()
		shards = append(shards, Shard{Name: label, Backend: backend})
		kv.AddShard(label, backend)
	}

	migration := ch.New()
	migration.SetBuckets(migrationBuckets)

	kv.BeginMigration(migration)

	// make sure requesting still works
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

	// make sure setting still works
	for i := 1; i < nElements; i++ {
		t.Logf("Storing test%d BASE refKey %d value%d", i, i, i)
		refKey := int64(i)
		err := kv.PutCell(context.TODO(), "test"+strconv.Itoa(i), "BASE", refKey, models.Cell{RefKey: refKey, Body: "value" + strconv.Itoa(i)})
		if err != nil {
			t.Fatal(err)
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

	// end the migration
	kv.EndMigration()

	// delete the old shards
	for i := 0; i < nShards; i++ {
		label := "test_shard" + strconv.Itoa(i)
		kv.DeleteShard(label)
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
