package db

import (
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	hello := "hello"
	original := hello
	Encode(&hello)
	Decode(&hello)
	if hello != original {
		t.Error("Unable to encode and decode: " + original)
	}
}

func TestEncodeDecodeWithNewline(t *testing.T) {
	newlinedrop := "\n!''' DROP TABLES EVERYWHERE"
	original := newlinedrop
	Encode(&newlinedrop)
	Decode(&newlinedrop)
	if newlinedrop != original {
		t.Error("Unable to encode and decode: " + original)
	}
}

func TestEncodeDecodeWithEOB(t *testing.T) {
	weirdness := "\xbd\xb2\x3d\x17\xbc\x20\xe2\x8c\x98"
	original := weirdness
	Encode(&weirdness)
	Decode(&weirdness)
	if weirdness != original {
		t.Error("Unable to encode and decode: " + original)
	}
}

func TestDecodeEOF(t *testing.T) {
	hex := "daaea4b2e9e4b738f18fe68b667e3bec7797598369"
	Decode(&hex)
}
