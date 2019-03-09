package badger

import (
	"github.com/rbastic/go-schemaless/storagetest"
	"io/ioutil"
	"os"
	"testing"
)

func TestBadger(t *testing.T) {

	dir, err := ioutil.TempDir(os.TempDir(), "schemaless-fs-storagetest")

	if err != nil {
		t.Skipf("Unable to create temporary directory: %s", err)
	}

	m, err := New(dir)

	if err != nil {
		t.Fatalf("unable to open badger db")
	}
	storagetest.StorageTest(t, m)

	// cleanup
	os.RemoveAll(dir)
}
