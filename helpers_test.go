package simplehstore

import (
	"testing"
)

func TestPostgresPrefix(t *testing.T) {
	Verbose = true
	a := "postgres://user:pass@0.0.0.0:5432/postgres?sslmode=disable"
	s, dbname := rebuildConnectionString(a)
	if a != s {
		t.Errorf("Error, the connection string could not be picked apart correctly: %s != %s", a, s)
	}
	if dbname != "postgres" {
		t.Errorf("Error, the connection string could not be picked apart correctly. dbname != postgres, but: %s", dbname)
	}
}

func TestArgs(t *testing.T) {
	Verbose = true
	a := "postgres://user:pass@0.0.0.0:5432/postgres?user=myuser&password=mypass&sslmode=enable"
	s, _ := rebuildConnectionString(a)
	if a != s {
		t.Errorf("Error, the connection string could not be picked apart correctly: %s != %s", a, s)
	}
}
