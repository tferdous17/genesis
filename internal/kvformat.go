package internal

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
	valueSize uint32
	valuePos  uint32
	timestamp uint32
}

func NewKeyEntry(timestamp, position, size uint32) KeyEntry {
	return KeyEntry{
		valueSize: size,
		valuePos:  position,
		timestamp: timestamp,
	}
}

func encodeHeader(timestamp, keySize, valueSize uint32) []byte {
	// impl
}

func decodeHeader(header []byte) (uint32, uint32, uint32) {
	// impl
}

func encodeKV(timestamp uint32, key string, value string) []byte {
	// impl
}

func decodeKV(data []byte) (uint32, string, string) {
	// impl
}
