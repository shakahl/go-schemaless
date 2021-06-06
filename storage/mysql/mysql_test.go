package mysql

import (
	"github.com/rbastic/go-schemaless/storagetest"
	"os"
	"testing"
)

const defaultPort = 3306

func TestMySQL(t *testing.T) {
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
		host = "localhost"
	}
	port := os.Getenv("SQLPORT")
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

	defer m.sugar.Sync()

	err = m.Open()
	if err != nil {
		t.Fatal(err)
	}
	storagetest.StorageTest(t, m)
}
