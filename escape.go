package db

import (
	"bytes"
	"compress/flate"
	"encoding/hex"
	"io/ioutil"
)

// Escape strings that are sent to MariaDB/MySQL by compressing and hex encoding
func Encode(sql string) string {
	var buf bytes.Buffer
	compressorWriter, err := flate.NewWriter(&buf, 1) // compression level 1 (fastest)
	if err != nil {
		panic(err.Error())
	}
	compressorWriter.Write([]byte(sql))
	compressorWriter.Close()
	return hex.EncodeToString(buf.Bytes())
}

// Dehex and decompress the given string
func Decode(code string) string {
	unhexedBytes, err := hex.DecodeString(code)
	if err != nil {
		panic(code + "    " + err.Error())
	}
	buf := bytes.NewBuffer(unhexedBytes)
	decompressorReader := flate.NewReader(buf)
	decompressedBytes, err := ioutil.ReadAll(decompressorReader)
	decompressorReader.Close()
	if err != nil {
		panic(err.Error())
	}
	return string(decompressedBytes)
}
