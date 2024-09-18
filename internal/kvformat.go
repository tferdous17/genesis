package internal

import (
	"encoding/binary"
	"fmt"
)

/*
The format for each key-value on disk is as follows:

	-------------------------------------------------

| timestamp | key_size | value_size | key | value |

	-------------------------------------------------

timestamp, key_size, value_size form the header of the entry and each of these must be 4 bytes at most
thus header size is fixed at a length of 12 bytes
*/
const headerSize = 12

// KeyEntry holds metadata about the KV pair, which is what we will insert into the keydir
type KeyEntry struct {
	timestamp     uint32
	valuePosition uint32
	valueSize     uint32
}

type Header struct {
	timestamp uint32
	keySize   uint32
	valueSize uint32
}

type Record struct {
	Header     Header
	key        string
	value      string
	recordSize uint32
}

func NewKeyEntry(timestamp, position, size uint32) KeyEntry {
	return KeyEntry{
		valueSize:     size,
		valuePosition: position,
		timestamp:     timestamp,
	}
}

func encodeHeader(timestamp, keySize, valueSize uint32) []byte {
	encodedHeader := make([]byte, headerSize)
	// writes binary representation of timestamp, keySize, valueSize into our bytes buffer
	_, err := binary.Encode(encodedHeader[:4], binary.LittleEndian, timestamp)
	_, err2 := binary.Encode(encodedHeader[4:8], binary.LittleEndian, keySize)
	_, err3 := binary.Encode(encodedHeader[8:12], binary.LittleEndian, keySize)

	if err != nil || err2 != nil || err3 != nil {
		fmt.Println("error encoding header", err, err2, err3)
	}
	return encodedHeader
}

func decodeHeader(header []byte) (uint32, uint32, uint32) {
	var timestamp, keySize, valueSize uint32

	// must pass in reference b/c go is call by value and won't modify original otherwise
	_, err := binary.Decode(header[:4], binary.LittleEndian, &timestamp)
	_, err2 := binary.Decode(header[4:8], binary.LittleEndian, &keySize)
	_, err3 := binary.Decode(header[8:12], binary.LittleEndian, &valueSize)

	if err != nil || err2 != nil || err3 != nil {
		fmt.Println("error decoding header", err, err2, err3)
	}

	return timestamp, keySize, valueSize
}

func EncodeKV(timestamp uint32, key string, value string) []byte {
	encodedKV := make([]byte, len(key)+len(value)) // key and val can only be 4 bytes long at most each
	encodedKV = append(encodedKV, []byte(key)...)
	encodedKV = append(encodedKV, []byte(value)...)

	_, err := binary.Encode(encodedKV, binary.LittleEndian, timestamp)
	if err != nil {
		fmt.Println("error encoding kv", err)
	}
	encodedKV = append(encodedKV)
	return encodedKV
}

func DecodeKV(data []byte) (uint32, string, string) {
	var timestamp uint32

	_, err := binary.Decode(data[:4], binary.LittleEndian, &timestamp)
	if err != nil {
		fmt.Println("error decoding", err)
	}
}
