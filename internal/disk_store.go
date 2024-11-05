package internal

import (
	"bitcask-go/utils"
	"bytes"
	"errors"
	"os"
	"time"
)

type DiskStore struct {
	memtable           *Memtable
	writeAheadLog      *os.File
	bucketManager      *BucketManager
	immutableMemtables []Memtable
}

type Operation int

const (
	PUT Operation = iota
	GET
	DELETE
)

const FlushSizeThreshold = 3_000

func NewDiskStore() (*DiskStore, error) {
	ds := &DiskStore{memtable: NewMemtable(), bucketManager: InitBucketManager()}

	logFile, err := os.OpenFile("../log/genesis_wal.log", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ds.writeAheadLog = logFile

	return ds, err
}

func (ds *DiskStore) Put(key string, value string) error {
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

	ds.memtable.Put(key, record)
	ds.appendOperationToWAL(PUT, record)

	// * Automatically flush when memtable reaches certain threshold
	if ds.memtable.sizeInBytes >= FlushSizeThreshold {
		ds.immutableMemtables = append(ds.immutableMemtables, deepCopyMemtable(*ds.memtable))
		ds.memtable.clear()
		ds.FlushMemtable()
	}

	return nil
}

func (ds *DiskStore) Get(key string) (string, error) {
	// log the get operation first
	ds.appendOperationToWAL(GET, Record{Key: key})

	// * Search memtable first, if not there -> search SSTables on disk
	record, err := ds.memtable.Get(key)
	if err == nil {
		return record.Value, nil
	} else if !errors.Is(err, utils.ErrKeyNotFound) {
		return "<!>", err
	} // else err is KeyNotFound

	// * key not found in memtable, thus search SSTables on disk
	return ds.bucketManager.RetrieveKey(key)
}

func (ds *DiskStore) Delete(key string) error {
	// * this is really just appending a new entry but with a tombstone value and empty key
	value := ""
	header := Header{
		TimeStamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
	}
	header.MarkTombstone()

	deletionRecord := Record{
		Header:     header,
		Key:        key,
		Value:      value,
		RecordSize: headerSize + header.KeySize + header.ValueSize,
	}
	deletionRecord.CalculateChecksum()

	ds.memtable.Put(key, deletionRecord)
	ds.appendOperationToWAL(DELETE, deletionRecord)

	return nil
}

func (ds *DiskStore) ListOfAllKeys() {
	ds.memtable.PrintAllRecords()
}

func (ds *DiskStore) FlushMemtable() {
	for i := range ds.immutableMemtables {
		sstable := ds.immutableMemtables[i].Flush("storage")
		ds.bucketManager.InsertTable(sstable)
		ds.immutableMemtables = ds.immutableMemtables[:i] // basically removing a "queued" memtable since its flushed
	}
}

func (ds *DiskStore) DebugMemtable() {
	ds.memtable.PrintAllRecords()
	utils.Logf("CURRENT SIZE IN BYTES: %d", ds.memtable.sizeInBytes)
}

func deepCopyMemtable(memtable Memtable) Memtable {
	deepCopy := NewMemtable()
	deepCopy.sizeInBytes = memtable.sizeInBytes

	// copy the tree data
	keys := memtable.data.Keys()
	values := memtable.data.Values()

	for i := range keys {
		deepCopy.data.Put(keys[i], values[i])
	}

	return *deepCopy
}

func (ds *DiskStore) appendOperationToWAL(op Operation, record Record) error {
	buf := new(bytes.Buffer)
	// Store operation as only 1 byte (only WAL entries will have this extra byte)
	buf.WriteByte(byte(op))

	// encode the entire key, value entry
	if encodeErr := record.EncodeKV(buf); encodeErr != nil {
		return utils.ErrEncodingKVFailed
	}

	// store in WAL
	if logErr := utils.WriteToFile(buf.Bytes(), ds.writeAheadLog); logErr != nil {
		return logErr
	}

	return nil
}
