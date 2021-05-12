package core

import (
	"context"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	ch "github.com/dgryski/go-shardedkv/choosers/chash"
	"github.com/rbastic/go-schemaless/models"
	st "github.com/rbastic/go-schemaless/storage/sqlite"
)

func TestSchemaless(t *testing.T) {
	var shards []Shard
	nElements := 2048
	nShards := 10

	for i := 0; i < nShards; i++ {
		label := "test_shard" + strconv.Itoa(i)

		dir, err := ioutil.TempDir(os.TempDir(), label)
		if err != nil {
			t.Skipf("Unable to create temporary directory: %s", err)
		}

		_ = os.Mkdir(dir, 0644)

		// TODO(rbastic): AddShard isn't used here?
		stor, err := st.New(dir)
		if err != nil {
			t.Fatal(err)
		}
		shards = append(shards, Shard{Name: label, Backend: stor})
	}

	chooser := ch.New()

	kv := New(chooser, shards)
	defer kv.Destroy(context.TODO())

	for i := 1; i < nElements; i++ {
		refKey := int64(i)
		err := kv.Put(context.TODO(), "test"+strconv.Itoa(i), "BASE", refKey, models.Cell{RefKey: refKey, Body: "value" + strconv.Itoa(i)})
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i < nElements; i++ {
		k := "test" + strconv.Itoa(i)

		v, ok, err := kv.GetLatest(context.TODO(), k, "BASE")
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

		dir, err := ioutil.TempDir(os.TempDir(), label)

		if err != nil {
			t.Skipf("Unable to create temporary directory: %s", err)
		}
		migrationBuckets = append(migrationBuckets, label)
		backend, err := st.New(dir)
		if err != nil {
			t.Fatal(err)
		}
		shards = append(shards, Shard{Name: label, Backend: backend})
		kv.AddShard(label, backend)
	}

	migration := ch.New()
	migration.SetBuckets(migrationBuckets)

	kv.BeginMigration(migration)

	// make sure requesting still works
	for i := 1; i < nElements; i++ {
		k := "test" + strconv.Itoa(i)

		v, ok, err := kv.GetLatest(context.TODO(), k, "BASE")
		if err != nil {
			t.Fatalf("failed to get key '%s': error: %s", k, err)
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
		err := kv.Put(context.TODO(), "test"+strconv.Itoa(i), "BASE", refKey, models.Cell{RefKey: refKey, Body: "value" + strconv.Itoa(i)})
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i < nElements; i++ {
		k := "test" + strconv.Itoa(i)

		v, ok, err := kv.GetLatest(context.TODO(), k, "BASE")
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

		v, ok, err := kv.GetLatest(context.TODO(), k, "BASE")
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

	err := kv.ResetConnection(context.TODO(), "")
	if err != nil {
		t.Fatal(err)
	}
}
