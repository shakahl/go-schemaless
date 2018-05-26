package mysql

import (
	"github.com/rbastic/go-schemaless/storagetest"
	"os"
	"testing"
)

func TestMySQL(t *testing.T) {
	// TODO(rbastic): Document how to bootstrap an installation to run these tests.
	user := os.Getenv("MYSQLUSER")
	if user == "" {
		panic("Please specify MYSQLUSER=...")
	}
	pass := os.Getenv("MYSQLPASS")
	if pass == "" {
		panic("Please specify MYSQLPASS=...")
	}
	host := os.Getenv("SQLHOST")
	if host == "" {
		panic("Please specify SQLHOST=...")
	}
	port := os.Getenv("MYSQLPORT")
	if port == "" {
		t.Log("Defaulted to port 3306.")
		port = "3306"
	} else {
		t.Logf("Using port %s", port)
	}
	db := os.Getenv("DB")
	if db == "" {
		panic("Please specify DB=...")
	}

	m := New(user, pass, host, port, db)
	storagetest.StorageTest(t, m)
}
