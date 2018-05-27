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
		t.Logf("Used port %s", port)
	}
	db := os.Getenv("DB")
	if db == "" {
		panic("Please specify DB=...")
	}

	m := New().WithUser(user).
		WithPass(pass).
		WithHost(host).
		WithPort(port).
		WithDatabase(db)

	err := m.WithZap()
	if err != nil {
		t.Fatal(err)
	}

	defer m.Sugar.Sync()

	err = m.Open()
	if err != nil {
		t.Fatal(err)
	}
	storagetest.StorageTest(t, m)
}
