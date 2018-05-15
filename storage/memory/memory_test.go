package memory

import (
	"github.com/rbastic/go-schemaless/storagetest"
	"testing"
)

func TestMemory(t *testing.T) {
	m := New()
	storagetest.StorageTest(t, m)
}
