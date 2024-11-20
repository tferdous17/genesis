package store

import (
	"bitcask-go/proto"
	"bitcask-go/utils"
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

type DiskStore struct {
	mu                 sync.Mutex
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

const FlushSizeThreshold = 1024 * 1024 * 256

// NewDiskStore starts up a single-node store
func NewDiskStore() (*DiskStore, error) {
	ds := &DiskStore{memtable: NewMemtable(), bucketManager: InitBucketManager()}

	logFile, err := os.OpenFile("../log/genesis_wal.log", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ds.writeAheadLog = logFile

	return ds, err
}

// NewDiskStoreDistributed starts up a cluster of N nodes
func NewDiskStoreDistributed(numOfNodes int) *Cluster {
	cluster := Cluster{}
	cluster.initNodes(numOfNodes)

	return &cluster
}

func (ds *DiskStore) Put(key *string, value *string) error {
	// lock access to the store so only 1 goroutine at a time can write to it, preventing race conditions
	ds.mu.Lock()
	defer ds.mu.Unlock()

	err := utils.ValidateKV(key, value)
	if err != nil {
		return err
	}

	// append key, value entry to disk
	header := Header{
		CheckSum:  0,
		Tombstone: 0,
		TimeStamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(*key)),
		ValueSize: uint32(len(*value)),
	}
	record := &Record{
		Header:     header,
		Key:        *key,
		Value:      *value,
		RecordSize: headerSize + header.KeySize + header.ValueSize,
	}
	record.Header.CheckSum = record.CalculateChecksum()

	ds.memtable.Put(key, record)
	// TODO: Batch WAL appends to improve performance, constant disk writes are too expensive
	//ds.appendOperationToWAL(PUT, record)

	// * Automatically flush when memtable reaches certain threshold
	if ds.memtable.sizeInBytes >= FlushSizeThreshold {
		ds.immutableMemtables = append(ds.immutableMemtables, *deepCopyMemtable(ds.memtable))
		ds.memtable.clear()
		ds.FlushMemtable()
	}

	return nil
}

func (ds *DiskStore) PutRecordFromGRPC(record *proto.Record) {
	rec := convertProtoRecordToStoreRecord(record)
	ds.memtable.Put(&record.Key, rec)
	fmt.Printf("stored proto record with key = %s into memtable", rec.Key)
}

func (ds *DiskStore) Get(key string) (string, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// log the get operation first
	//ds.appendOperationToWAL(GET, &Record{Key: key})

	// * Search memtable first, if not there -> search SSTables on disk
	record, err := ds.memtable.Get(&key)
	if err == nil {
		return record.Value, nil
	} else if !errors.Is(err, utils.ErrKeyNotFound) {
		return "<!>", err
	} // else err is KeyNotFound

	// * key not found in memtable, thus search SSTables on disk
	return ds.bucketManager.RetrieveKey(&key)
}

func (ds *DiskStore) Delete(key string) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

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

	ds.memtable.Put(&key, &deletionRecord)
	ds.appendOperationToWAL(DELETE, &deletionRecord)

	return nil
}

func (ds *DiskStore) LengthOfMemtable() {
	fmt.Println(len(ds.memtable.data.Keys()))
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

func deepCopyMemtable(memtable *Memtable) *Memtable {
	deepCopy := NewMemtable()
	deepCopy.sizeInBytes = memtable.sizeInBytes

	// copy the tree data
	keys := memtable.data.Keys()
	values := memtable.data.Values()

	for i := range keys {
		deepCopy.data.Put(keys[i], values[i])
	}

	return deepCopy
}

func (ds *DiskStore) Close() bool {
	//TODO implement me
	return true
}

func (ds *DiskStore) appendOperationToWAL(op Operation, record *Record) error {
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
