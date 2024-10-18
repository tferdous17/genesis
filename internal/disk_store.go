package internal

import (
	"bitcask-go/utils"
	"bytes"
	"errors"
	"fmt"
	"os"
	"time"
)

type DiskStore struct {
	memtable           *Memtable
	writeAheadLog      *os.File
	buckets            []Bucket
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
	ds := &DiskStore{memtable: NewMemtable()}

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

	// encode the entire key, value entry
	buf := new(bytes.Buffer)
	// Store operation as only 1 byte (only WAL entries will have this extra byte)
	buf.WriteByte(byte(PUT))
	if encodeErr := record.EncodeKV(buf); encodeErr != nil {
		return utils.ErrEncodingKVFailed
	}
	// store in WAL
	logErr := utils.WriteToFile(buf.Bytes(), ds.writeAheadLog)
	if logErr != nil {
		fmt.Println(logErr)
	}

	// * Automatically flush when memtable reaches certain threshold
	if ds.memtable.sizeInBytes >= FlushSizeThreshold {
		ds.immutableMemtables = append(ds.immutableMemtables, deepCopyMemtable(*ds.memtable))
		ds.memtable.clear()
		ds.FlushMemtable()
	}

	return nil
}

func (ds *DiskStore) Get(key string) (string, error) {
	// * Search memtable first, if not there -> search SSTables on disk
	record, err := ds.memtable.Get(key)
	if err == nil {
		return record.Value, nil
	} else if !errors.Is(err, utils.ErrKeyNotFound) {
		return "<!>", err
	} // else err is KeyNotFound

	// ! key not found in memtable, search SSTables on disk
	// * search the most RECENT sstable first
	for i := 0; i < len(ds.buckets); i++ {
		for j := 0; j < len(ds.buckets[i].tables); j++ {
			value, err := ds.buckets[i].tables[j].Get(key)
			if errors.Is(err, utils.ErrKeyNotWithinTable) {
				continue
			}
			return value, err
		}
	}
	return "<!not_found>", utils.ErrKeyNotFound
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

	// encode the entire key, value entry
	buf := new(bytes.Buffer)
	// Store operation as only 1 byte (only WAL entries will have this extra byte)
	buf.WriteByte(byte(DELETE))
	if encodeErr := deletionRecord.EncodeKV(buf); encodeErr != nil {
		return utils.ErrEncodingKVFailed
	}

	// store in WAL
	logErr := utils.WriteToFile(buf.Bytes(), ds.writeAheadLog)
	if logErr != nil {
		fmt.Println(logErr)
	}

	return nil
}

func (ds *DiskStore) ListOfAllKeys() {
	ds.memtable.PrintAllRecords()
}

var counter int = 0

func (ds *DiskStore) FlushMemtable() {
	for i := range ds.immutableMemtables {
		counter++
		utils.Logf("SIZE AT TIME OF FLUSHING (#%d): %d\n", counter, int(ds.immutableMemtables[i].sizeInBytes))
		sstable := ds.immutableMemtables[i].Flush("storage")
		if len(ds.buckets) == 0 {
			// ! buckets is empty. so we cant append to a nonexistent index
			// * create new bucket and append table
			ds.buckets = append(ds.buckets, *InitBucket(sstable))
		} else {
			ds.buckets[0].AppendTableToBucket(sstable)
		}
		utils.LogRED("Len of bucket 0: %d", len(ds.buckets[0].tables))
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
