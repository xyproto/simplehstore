package simplehstore

import (
	"testing"
)

func TestPostgresPrefix(t *testing.T) {
	Verbose = true
	a := "postgres://user:pass@0.0.0.0:5432/postgres?sslmode=disable"
	s, dbname := rebuildConnectionString(a, true)
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
	s, _ := rebuildConnectionString(a, true)
	if a != s {
		t.Errorf("Error, the connection string could not be picked apart correctly: %s != %s", a, s)
	}
}

func TestTravisDSN(t *testing.T) {
	Verbose = true
	a := "postgres:@127.0.0.1"
	b := "postgres://postgres@127.0.0.1:5432/test?sslmode=disable"
	s, _ := rebuildConnectionString(a, true)
	if s != b {
		t.Errorf("Error, the connection string could not be picked apart correctly:\n\t%s !=\n\t%s\ngiven %s", s, b, a)
	}
}

func TestTravisDSN2(t *testing.T) {
	Verbose = true
	a := "postgres:@127.0.0.1"
	b := "postgres://postgres@127.0.0.1:5432/?sslmode=disable"
	s, _ := rebuildConnectionString(a, false)
	if s != b {
		t.Errorf("Error, the connection string could not be picked apart correctly:\n\t%s !=\n\t%s\ngiven %s", s, b, a)
	}
}
