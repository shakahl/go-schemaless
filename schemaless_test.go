package schemaless

import (
	"context"
	"github.com/rbastic/go-schemaless/core"
	"github.com/rbastic/go-schemaless/models"
	st "github.com/rbastic/go-schemaless/storage/sqlite"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

func TestSchemaless(t *testing.T) {
	var shards []core.Shard
	nElements := 1000
	nShards := 10

	for i := 0; i < nShards; i++ {
		label := "test_shard" + strconv.Itoa(i)
		dir, err := ioutil.TempDir(os.TempDir(), label)

		if err != nil {
			t.Skipf("Unable to create temporary directory: %s", err)
		}

		// TODO(rbastic): AddShard isn't used here?
		stor, err := st.New(dir)
		if err != nil {
			t.Fatal(err)
		}
		shards = append(shards, core.Shard{Name: label, Backend: stor})
	}

	kv := New().WithSource(shards)
	defer kv.Destroy(context.TODO())

	for i := 1; i < nElements; i++ {
		refKey := int64(i)
		err := kv.PutCell(context.TODO(), "test"+strconv.Itoa(i), "BASE", refKey, models.Cell{RefKey: refKey, Body: "value" + strconv.Itoa(i)})
		if err != nil {
			t.Fatal(err)
		}
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
