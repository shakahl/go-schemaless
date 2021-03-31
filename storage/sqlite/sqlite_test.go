package sqlite

import (
	"github.com/rbastic/go-schemaless/storagetest"
	"io/ioutil"
	"os"
	"testing"
)

func TestSQLite(t *testing.T) {

	dir, err := ioutil.TempDir(os.TempDir(), "schemaless-fs-storagetest")

	if err != nil {
		t.Skipf("Unable to create temporary directory: %s", err)
	}

	m, err := New(dir)
	if err != nil {
		t.Skipf("Unable to create sqlite storage adapter: %s", err)
	}
	storagetest.StorageTest(t, m)

	// cleanup
	os.RemoveAll(dir)
}
