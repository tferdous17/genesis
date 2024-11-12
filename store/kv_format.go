package store

import (
	"bitcask-go/utils"
	"bytes"
	"encoding/binary"
	"hash/crc32"
)

/*
The format for each key-value on disk is as follows:

| timestamp | key_size | value_size | key | value |

timestamp, key_size, value_size form the header of the entry and each of these must be 4 bytes at most
thus header size is fixed at a length of 12 bytes
*/
const headerSize = 17

// KeyEntry holds metadata about the KV pair, which is what we will insert into the keydir
type KeyEntry struct {
	TimeStamp     uint32
	ValuePosition uint32
	EntrySize     uint32
}

type Header struct {
	CheckSum  uint32
	Tombstone uint8
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
	err := binary.Write(buf, binary.LittleEndian, &h.CheckSum)
	binary.Write(buf, binary.LittleEndian, &h.Tombstone)
	binary.Write(buf, binary.LittleEndian, &h.TimeStamp)
	binary.Write(buf, binary.LittleEndian, &h.KeySize)
	binary.Write(buf, binary.LittleEndian, &h.ValueSize)

	if err != nil {
		return utils.ErrEncodingHeaderFailed
	}

	return nil
}

func (h *Header) DecodeHeader(buf []byte) error {
	// must pass in reference b/c go is call by value and won't modify original otherwise
	_, err := binary.Decode(buf[:4], binary.LittleEndian, &h.CheckSum)
	binary.Decode(buf[4:5], binary.LittleEndian, &h.Tombstone)
	binary.Decode(buf[5:9], binary.LittleEndian, &h.TimeStamp)
	binary.Decode(buf[9:13], binary.LittleEndian, &h.KeySize)
	binary.Decode(buf[13:17], binary.LittleEndian, &h.ValueSize)

	if err != nil {
		return utils.ErrDecodingHeaderFailed
	}

	return nil
}

func (h *Header) MarkTombstone() {
	h.Tombstone = 1
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
	r.Key = string(buf[headerSize : headerSize+r.Header.KeySize])
	r.Value = string(buf[headerSize+r.Header.KeySize : headerSize+r.Header.KeySize+r.Header.ValueSize])
	r.RecordSize = headerSize + r.Header.KeySize + r.Header.ValueSize
	return err
}

func (r *Record) Size() uint32 {
	return r.RecordSize
}

func (r *Record) CalculateChecksum() uint32 {
	/*
		compute a checksum for the ENTIRE entry, not just the value itself
		i suppose this is more secure? since it accounts for if ANY fields change

		crc.checksum takes a []byte buf as input, so lets try encoding the entire record into a
		byte buffer and then calculate the checksum based on that
		we only want to calculate it from tstamp...value so go from [4:]
	*/
	headerBuf := new(bytes.Buffer)
	binary.Write(headerBuf, binary.LittleEndian, &r.Header.Tombstone)
	binary.Write(headerBuf, binary.LittleEndian, &r.Header.TimeStamp)
	binary.Write(headerBuf, binary.LittleEndian, &r.Header.KeySize)
	binary.Write(headerBuf, binary.LittleEndian, &r.Header.ValueSize)

	// cant append a []byte directly into a []byte, so destructure the val into indiv bytes
	kvBuf := append([]byte(r.Key), []byte(r.Value)...)

	buf := append(headerBuf.Bytes(), kvBuf...)

	return crc32.ChecksumIEEE(buf)
}
