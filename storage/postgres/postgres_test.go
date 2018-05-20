package postgres

import (
	"github.com/rbastic/go-schemaless/storagetest"
	"os"
	"testing"
)

func TestPostgres(t *testing.T) {
	// TODO(rbastic): Document how to bootstrap an installation to run these tests.
	user := os.Getenv("SQLUSER")
	if user == "" {
		panic("Please specify SQLUSER=...")
	}
	pass := os.Getenv("SQLPASS")
	if pass == "" {
		panic("Please specify SQLPASS=...")
	}
	host := os.Getenv("SQLHOST")
	if host == "" {
		panic("Please specify SQLHOST=...")
	}
	// TODO(rbastic): No custom PORT support for postgres. Laziness.
	db := os.Getenv("DB")
	if db == "" {
		panic("Please specify DB=...")
	}

	m := New(user, pass, host, "", db)
	storagetest.StorageTest(t, m)
}
