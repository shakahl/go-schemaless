package rqlite

import (
	"github.com/rbastic/go-schemaless/storagetest"
	"testing"
)

func TestRQLite(t *testing.T) {
	m := New().WithZap().WithURL("http://")
	storagetest.StorageTest(t, m)
}
