package db

import (
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	hello := "hello"
	if Decode(Encode(hello)) != hello {
		t.Error("Unable to encode and decode: " + hello)
	}
}

func TestEncodeDecodeWithNewline(t *testing.T) {
	newlinedrop := "\n!''' DROP TABLES EVERYWHERE"
	if Decode(Encode(newlinedrop)) != newlinedrop {
		t.Error("Unable to encode and decode: " + newlinedrop)
	}
}

func TestEncodeDecodeWithEOB(t *testing.T) {
	weirdness := "\xbd\xb2\x3d\x17\xbc\x20\xe2\x8c\x98"
	if Decode(Encode(weirdness)) != weirdness {
		t.Error("Unable to encode and decode: " + weirdness)
	}
}
