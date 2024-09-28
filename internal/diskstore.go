package internal

import (
	"bitcask-go/utils"
	"bytes"
	"errors"
	"io"
	"os"
	"time"
)

/*
notes:
ok so a bitcask on disk is just a directory (our databse server),
with multiple files inside it
	-> 1 active file, 0 or more inactive files

ok so how do we actually create the bitcask?
	-> single file on disk called the "main database server"
	-> this file will contain 1 or more data files (active/inactive)

within each data file:
	-> data format is: tstamp | ksz | value_sz | key | val
	-> a data file is nothing more than a linear sequence of the above entries

*note: the active data file will automatically close once it reaches a certain size threshold

this is DISK storage, so this will all be stored in SSD/HDD, therefore being persistent
*/

type DiskStore struct {
	serverFile *os.File
	// writePosition will tell us the current "cursor" position
	// in the file to start reading from, default val is 0
	writePosition int
	// in-memory keydir that allows us to find the data we're looking for in disk
	keyDir map[string]KeyEntry
}

func fileExists(fileName string) bool {
	if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
		return false // file does not exist
	}
	return true
}

func NewDiskStore(fileName string) (*DiskStore, error) {
	ds := &DiskStore{keyDir: make(map[string]KeyEntry)}
	if fileExists(fileName) {
		// populate keydir for existing store
		err := ds.initKeyDir(fileName)
		if err != nil {
			return nil, utils.ErrKeyDirInit
		}
	}

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ds.serverFile = file

	return ds, err
}

func (ds *DiskStore) Put(key string, value string) error {
	_, ok := ds.keyDir[key]
	if ok {
		return utils.ErrDuplicateKey
	}

	err := utils.ValidateKV(key, value)
	if err != nil {
		return err
	}

	// append key, value entry to disk
	header := Header{
		CheckSum:  0,
		Tombstone: 0,
		TimeStamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
	}
	record := Record{
		Header:     header,
		Key:        key,
		Value:      value,
		RecordSize: headerSize + header.KeySize + header.ValueSize,
	}
	record.Header.CheckSum = record.CalculateChecksum()

	// encode the entire key, value entry
	buf := new(bytes.Buffer)
	if encodeErr := record.EncodeKV(buf); encodeErr != nil {
		return utils.ErrEncodingKVFailed
	}

	// write to disk
	ds.writeToFile(buf.Bytes())

	// now update keydir
	ds.keyDir[key] = NewKeyEntry(header.TimeStamp, uint32(ds.writePosition), record.RecordSize)
	ds.writePosition += int(record.RecordSize)

	return nil
}

func (ds *DiskStore) Get(key string) (string, error) {
	/*
		lookup key in keydir
		if not exist:
			return key not found
		else:
			create buffer the same size as the kv entry
			read bytes from value position to valueSize
			decode the buffer into a record
			return record.Value
	*/
	keyEntry, ok := ds.keyDir[key]
	if !ok {
		return "", utils.ErrKeyNotFound
	}

	// EntrySize for "othello" -> "shakespeare"
	// should be 35: headerSize(17) + keySize(7) + valueSize(11) = 35
	entireEntry := make([]byte, keyEntry.EntrySize)

	// read 35 bytes from the file starting from valuePosition (0 in this case)
	ds.serverFile.ReadAt(entireEntry, int64(keyEntry.ValuePosition))

	// ok now let's decode the entireEntry buffer into a record
	record := Record{}
	if decodeErr := record.DecodeKV(entireEntry); decodeErr != nil {
		return "", utils.ErrDecodingKVFailed
	}

	return record.Value, nil
}

func (ds *DiskStore) Delete(key string) error {
	// key note: this is an APPEND-ONLY db, so it wouldn't make sense to
	// overwrite existing data and place a tombstone value there
	// thus we have to write a semi-copy of the record w/ the tombstone val activated

	_, ok := ds.keyDir[key]
	if !ok {
		return utils.ErrKeyNotFound
	}

	tempVal := ""
	header := Header{
		CheckSum:  0,
		TimeStamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(tempVal)),
	}
	header.MarkTombstone()

	record := Record{
		Header:     header,
		Key:        key,
		Value:      tempVal,
		RecordSize: headerSize + header.KeySize + header.ValueSize,
	}
	record.Header.CheckSum = record.CalculateChecksum()

	buf := new(bytes.Buffer)
	if encodeErr := record.EncodeKV(buf); encodeErr != nil {
		return utils.ErrEncodingKVFailed
	}
	ds.writeToFile(buf.Bytes())

	delete(ds.keyDir, key)

	return nil
}

func (ds *DiskStore) Close() bool {
	// important to actually write to disk thru Sync() first
	ds.serverFile.Sync()
	if err := ds.serverFile.Close(); err != nil {
		return false
	}
	return true
}

func (ds *DiskStore) initKeyDir(existingFile string) error {
	file, _ := os.Open(existingFile)
	defer file.Close()

	for {
		// read 12 bytes from our and store it into header
		header := make([]byte, headerSize)
		_, err := io.ReadFull(file, header)

		// error above could be EOF or some other error, so handle either case
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// following func will decode the buffer into a Header{}
		h, err := NewHeader(header)
		if err != nil {
			return err
		}
		// read key, val into respective buffers
		key := make([]byte, h.KeySize)
		value := make([]byte, h.ValueSize)

		_, keyErr := io.ReadFull(file, key)
		if keyErr != nil {
			return err
		}

		_, valErr := io.ReadFull(file, value)
		if valErr != nil {
			return err
		}
		// total size of this key, val entry including header
		totalSize := headerSize + h.KeySize + h.ValueSize
		ds.keyDir[string(key)] = NewKeyEntry(h.TimeStamp, uint32(ds.writePosition), totalSize)
		ds.writePosition += int(totalSize)
	}
	return nil
}

func (ds *DiskStore) writeToFile(data []byte) error {
	// i want to panic on these errors b/c its bad if our data isnt writing
	if _, writeErr := ds.serverFile.Write(data); writeErr != nil {
		panic(writeErr)
	}
	// VERY important to call Sync, b/c this flushes the in-memory buffer of our file to the disk
	// this is what actually makes our data persist as the data is initially stored in said buffer
	// before reaching disk
	if syncErr := ds.serverFile.Sync(); syncErr != nil {
		panic(syncErr)
	}
	return nil
}

func (ds *DiskStore) ListOfAllKeys() []string {
	list := make([]string, 0, len(ds.keyDir))
	for k, _ := range ds.keyDir {
		list = append(list, k)
	}
	return list
}
