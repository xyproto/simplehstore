package db

import (
	"bytes"
	"compress/flate"
	"encoding/hex"
	//"encoding/base32"
	//"encoding/base64"
	"io/ioutil"
)

// MariaDB/MySQL does not handle some characters well.
// Compressing and hex encoding the value is one of many possible ways
// to avoid this. Using BLOB fields and different datatypes is another.
func Encode(value string) string {
	// Don't encode empty strings
	if value == "" {
		return ""
	}
	var buf bytes.Buffer
	compressorWriter, err := flate.NewWriter(&buf, 1) // compression level 1 (fastest)
	if err != nil {
		panic(err.Error())
	}
	compressorWriter.Write([]byte(value))
	compressorWriter.Close()
	return hex.EncodeToString(buf.Bytes())
	//return base32.StdEncoding.EncodeToString(buf.Bytes())
}

// Dehex and decompress the given string
func Decode(code string) string {
	// Don't decode empty strings
	if code == "" {
		return ""
	}
	unhexedBytes, err := hex.DecodeString(code)
	//unhexedBytes, err := base32.StdEncoding.DecodeString(code)
	if err != nil {
		panic("Could not hexdecode " + code + ": " + err.Error())
	}
	buf := bytes.NewBuffer(unhexedBytes)
	decompressorReader := flate.NewReader(buf)
	decompressedBytes, err := ioutil.ReadAll(decompressorReader)
	decompressorReader.Close()
	if err != nil {
		panic(err.Error())
	}
	value := string(decompressedBytes)
	return value
}
