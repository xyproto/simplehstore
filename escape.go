package db

import (
	"bytes"
	"compress/flate"
	"encoding/hex"
	"io/ioutil"
	//"strings"
)

// MariaDB/MySQL does not handle some characters well.
// Compressing and hex encoding the value should avoid this.
func Encode(value string) string {
	if value == "" {
		return ""
	}
	var buf bytes.Buffer
	//value = strings.Replace(value, "\023", "-=-=-", -1)
	compressorWriter, err := flate.NewWriter(&buf, 1) // compression level 1 (fastest)
	if err != nil {
		panic(err.Error())
	}
	compressorWriter.Write([]byte(value))
	compressorWriter.Close()
	return hex.EncodeToString(buf.Bytes())
}

// Dehex and decompress the given string
func Decode(code string) string {
	if code == "" {
		return ""
	}
	unhexedBytes, err := hex.DecodeString(code)
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
	//return strings.Replace(value, "-=-=-", "\023", -1)
	return value
}
