package fs

import (
	"github.com/rbastic/go-schemaless/storagetest"
	"io/ioutil"
	"os"
	"testing"
)

func TestFS(t *testing.T) {

	dir, err := ioutil.TempDir(os.TempDir(), "schemaless-fs-storagetest")

	if err != nil {
		t.Skipf("Unable to create temporary directory: %s", err)
	}

	m := New(dir)
	storagetest.StorageTest(t, m)

	// cleanup
	os.RemoveAll(dir)
}
