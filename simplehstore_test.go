package simplehstore

import (
	"testing"
)

const (
	testdata1 = "abc123"
	testdata2 = "def456"
	testdata3 = "ghi789"
)

func TestLocalConnection(t *testing.T) {
	Verbose = true

	//err := TestConnection() // locally
	err := TestConnectionHost(defaultConnectionString)
	if err != nil {
		t.Error(err.Error())
	}
}

func TestTwoFields(t *testing.T) {
	test, test23, ok := twoFields("test1@test2@test3", "@")
	if ok && ((test != "test1") || (test23 != "test2@test3")) {
		t.Error("Error in twoFields functions")
	}
}
