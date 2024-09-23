package internal

import (
	"bitcask-go/utils"
	"bytes"
	"encoding/binary"
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
	TimeStamp     uint32
	ValuePosition uint32
	EntrySize     uint32
}

type Header struct {
	TimeStamp uint32
	KeySize   uint32
	ValueSize uint32
}

type Record struct {
	Header     Header
	Key        string
	Value      string
	RecordSize uint32
}

func NewKeyEntry(timestamp, position, size uint32) KeyEntry {
	return KeyEntry{
		TimeStamp:     timestamp,
		ValuePosition: position,
		EntrySize:     size,
	}
}

func NewHeader(buf []byte) (*Header, error) {
	header := &Header{}
	err := header.DecodeHeader(buf)

	if err != nil {
		return nil, err
	}

	return header, err
}

func (h *Header) EncodeHeader(buf *bytes.Buffer) error {
	err := binary.Write(buf, binary.LittleEndian, &h.TimeStamp)
	binary.Write(buf, binary.LittleEndian, &h.KeySize)
	binary.Write(buf, binary.LittleEndian, &h.ValueSize)

	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}

	return nil
}

func (h *Header) DecodeHeader(buf []byte) error {

	// must pass in reference b/c go is call by value and won't modify original otherwise
	_, err := binary.Decode(buf[:4], binary.LittleEndian, &h.TimeStamp)
	binary.Decode(buf[4:8], binary.LittleEndian, &h.KeySize)
	binary.Decode(buf[8:12], binary.LittleEndian, &h.ValueSize)

	if err != nil {
		return utils.ErrDecodingHeaderFailed
	}

	return nil
}

func (r *Record) EncodeKV(buf *bytes.Buffer) error {
	// write the KV data into the buffer
	r.Header.EncodeHeader(buf)
	buf.WriteString(r.Key)
	_, err := buf.WriteString(r.Value)
	return err
}

func (r *Record) DecodeKV(buf []byte) error {
	err := r.Header.DecodeHeader(buf[:headerSize])
	// now lets figure out the offsets for key and values so we know what to decode from the byte arr
	r.Key = string(buf[headerSize : headerSize+r.Header.KeySize])
	r.Value = string(buf[headerSize+r.Header.KeySize : headerSize+r.Header.KeySize+r.Header.ValueSize])
	r.RecordSize = headerSize + r.Header.KeySize + r.Header.ValueSize
	return err
}

func (r *Record) Size() uint32 {
	return r.RecordSize
}
