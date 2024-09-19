package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

/*
The format for each key-value on disk is as follows:

| timestamp | key_size | value_size | key | value |

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
		timestamp:     timestamp,
		valuePosition: position,
		valueSize:     size,
	}
}

func (h *Header) EncodeHeader(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, &h.timestamp)
	err2 := binary.Write(buf, binary.LittleEndian, &h.keySize)
	err3 := binary.Write(buf, binary.LittleEndian, &h.valueSize)

	if err2 != nil || err3 != nil {
		fmt.Println("error encoding header")
	}

	return err
}

func (h *Header) DecodeHeader(buf []byte) error {

	// must pass in reference b/c go is call by value and won't modify original otherwise
	_, err := binary.Decode(buf[:4], binary.LittleEndian, &h.timestamp)
	_, err2 := binary.Decode(buf[4:8], binary.LittleEndian, &h.keySize)
	_, err3 := binary.Decode(buf[8:12], binary.LittleEndian, &h.keySize)

	if err2 != nil || err3 != nil {
		fmt.Println("error decoding header")
	}

	return err
}

func (r *Record) EncodeKV(buf *bytes.Buffer) error {
	// write the KV data into the buffer
	_, err := buf.WriteString(r.key)
	buf.WriteString(r.value)
	return err
}

func (r *Record) DecodeKV(buf []byte) error {
	err := r.Header.DecodeHeader(buf[:headerSize])
	// now lets figure out the offsets for key and values so we know what to decode from the byte arr
	r.key = string(buf[headerSize : headerSize+r.Header.keySize])
	r.value = string(buf[headerSize : headerSize+r.Header.keySize+r.Header.valueSize])
	r.recordSize = headerSize + r.Header.keySize + r.Header.valueSize
	return err
}

func (r *Record) Size() uint32 {
	return r.recordSize
}
