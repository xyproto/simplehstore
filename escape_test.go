package db

import (
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	if Decode(Encode("hello")) != "hello" {
		t.Error("Unable to encode and decode.")
	}
	if Decode(Encode("\n!''' DROP TABLES EVERYWHERE")) != "\n!''' DROP TABLES EVERYWHERE" {
		t.Error("Unable to encode and decode.")
	}
}
