package postgres

import (
	"github.com/rbastic/go-schemaless/storagetest"
	"os"
	"testing"
)

func TestPostgres(t *testing.T) {
	// TODO(rbastic): Document how to bootstrap an installation to run these tests.
	user := os.Getenv("PGUSER")
	if user == "" {
		panic("Please specify SQLUSER=...")
	}
	pass := os.Getenv("PGPASS")
	if pass == "" {
		panic("Please specify SQLPASS=...")
	}
	host := os.Getenv("PGHOST")
	if host == "" {
		host = "localhost"
	}
	// TODO(rbastic): No custom PORT support for postgres. Laziness.
	db := os.Getenv("DB")
	if db == "" {
		panic("Please specify DB=...")
	}

	m := New(user, pass, host, "", db)
	storagetest.StorageTest(t, m)
}
