package storagetest

import (
	"errors"
	"testing"

	"github.com/rbastic/go-schemaless"
	"github.com/rbastic/go-schemaless/models/cell"
)

type Errstore struct{}

func (e Errstore) Get(key string) ([]byte, bool, error) {
	return nil, false, errors.New("error storage get")
}
func (e Errstore) Set(key string, val []byte) error { return errors.New("error storage Set") }
func (e Errstore) Delete(key string) (bool, error)  { return false, errors.New("error storage Delete") }
func (e Errstore) ResetConnection(key string) error {
	return errors.New("error storage ResetConnection")
}

// StorageTest is a simple sanity check for a schemaless Storage backend
func StorageTest(t *testing.T, storage schemaless.Storage) {
	v, ok := storage.GetCell("hello", "BASE", 1)
	if ok {
		t.Errorf("getting a non-existent key was 'ok': v=%v ok=%v\n", v, ok)
	}

	storage.PutCell("hello", "BASE", 1, models.Cell{Body: []byte("wowza")})

	v, ok = storage.GetCellLatest("hello", "BASE")
	if !ok || string(v.Body) != "wowza" {
		t.Errorf("failed getting a valid key: v=%v ok=%v\n", v, ok)
	}

	err := storage.ResetConnection("hello")
	if err != nil {
		t.Errorf("failed resetting connection for key: err=%v\n", err)
	}
}
