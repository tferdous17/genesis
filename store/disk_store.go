package store

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/tferdous17/genesis/utils"

	"github.com/tferdous17/genesis/proto"
)

type DiskStore struct {
	mu                 sync.Mutex
	memtable           *Memtable
	writeAheadLog      *writeAheadLog
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

// NewCluster starts up a cluster of N nodes (stores), internally calls the newStore method per node
func NewCluster(numOfNodes uint32) *Cluster {
	cluster := Cluster{}
	cluster.initNodes(numOfNodes)

	return &cluster
}

// newStore starts up a single-node KV store
func newStore(nodeId string) (*DiskStore, error) {
	ds := &DiskStore{
		memtable:      NewMemtable(nodeId),
		bucketManager: InitBucketManager(),
	}

	logFile, err := os.OpenFile(
		fmt.Sprintf("../log/genesis_wal-%s.log", nodeId),
		os.O_APPEND|os.O_RDWR|os.O_CREATE,
		0666,
	)
	if err != nil {
		return nil, fmt.Errorf("open WAL file for node %s: %w", nodeId, err)
	}
	ds.writeAheadLog = &writeAheadLog{file: logFile}

	return ds, nil
}

func (ds *DiskStore) Put(key string, value string) error {
	// lock access to the store so only 1 goroutine at a time can write to it, preventing race conditions
	if ds == nil {
		return fmt.Errorf("disk store is not initialized")
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.memtable == nil {
		return fmt.Errorf("memtable is not initialized")
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
	record := &Record{
		Header:     header,
		Key:        key,
		Value:      value,
		RecordSize: headerSize + header.KeySize + header.ValueSize,
	}
	record.Header.CheckSum, err = record.CalculateChecksum()
	if err != nil {
		return err
	}

	ds.memtable.Put(key, record)
	err = ds.writeAheadLog.appendWALOperation(PUT, record)
	if err != nil {
		return err
	}

	// * Automatically flush when memtable reaches certain threshold
	if ds.memtable.sizeInBytes >= FlushSizeThreshold {
		// Storing a shallow copy of the memtable's struct values into this slice
		// (doesn't deep copy the memtable's inner tree itself, just the ref to it, keeps it intact)
		ds.immutableMemtables = append(ds.immutableMemtables, *ds.memtable)
		ds.memtable = NewMemtable(ds.memtable.nodeId)
		if err := ds.FlushMemtable(); err != nil {
			return fmt.Errorf("flush memtable: %w", err)
		}
	}

	return nil
}

func (ds *DiskStore) PutRecordFromGRPC(record *proto.Record) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	rec := convertProtoRecordToStoreRecord(record)
	ds.memtable.Put(rec.Key, rec)

	if err := ds.writeAheadLog.appendWALOperation(PUT, rec); err != nil {
		return fmt.Errorf("append to WAL: %w", err)
	}

	if ds.memtable.sizeInBytes >= FlushSizeThreshold {
		ds.immutableMemtables = append(ds.immutableMemtables, *ds.memtable)
		ds.memtable = NewMemtable(ds.memtable.nodeId)
		if err := ds.FlushMemtable(); err != nil {
			return fmt.Errorf("flush memtable: %w", err)
		}
	}

	return nil
}

func (ds *DiskStore) Get(key string) (string, error) {
	if ds == nil {
		return "", fmt.Errorf("disk store is not initialized")
	}
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// * Search memtable first, if not there -> search SSTables on disk
	record, err := ds.memtable.Get(key)
	if err == nil {
		return record.Value, nil
	} else if !errors.Is(err, utils.ErrKeyNotFound) {
		return "", err
	} // else err is KeyNotFound

	// * key not found in memtable, thus search SSTables on disk
	return ds.bucketManager.RetrieveKey(key)
}

func (ds *DiskStore) Delete(key string) error {
	if ds == nil {
		return fmt.Errorf("disk store is not initialized")
	}
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
	var err error
	deletionRecord.Header.CheckSum, err = deletionRecord.CalculateChecksum()
	if err != nil {
		return err
	}

	ds.memtable.Put(key, &deletionRecord)
	if err := ds.writeAheadLog.appendWALOperation(DELETE, &deletionRecord); err != nil {
		return fmt.Errorf("append delete to WAL: %w", err)
	}

	return nil
}

func (ds *DiskStore) LengthOfMemtable() {
	fmt.Println(len(ds.memtable.data.Keys()))
}

func (ds *DiskStore) FlushMemtable() error {
	for i := range ds.immutableMemtables {
		sstable, err := ds.immutableMemtables[i].Flush("storage")
		if err != nil {
			ds.immutableMemtables = ds.immutableMemtables[i:]
			return fmt.Errorf("flush memtable at index %d: %w", i, err)
		}

		if err := ds.bucketManager.InsertTable(sstable); err != nil {
			// Retain remaining memtables upon error so they can be still be flushed later
			ds.immutableMemtables = ds.immutableMemtables[i:]
			return fmt.Errorf("flush memtable at index %d: %w", i, err)
		}
	}
	// By this point all memtables were successfully flushed, so clear the slice
	ds.immutableMemtables = ds.immutableMemtables[:0]
	return nil
}

func (ds *DiskStore) DebugMemtable() {
	ds.memtable.PrintAllRecords()
	utils.Logf("CURRENT SIZE IN BYTES: %d", ds.memtable.sizeInBytes)
}

func deepCopyMemtable(memtable *Memtable) *Memtable {
	deepCopy := NewMemtable(memtable.nodeId)
	deepCopy.sizeInBytes = memtable.sizeInBytes

	// copy the tree data
	keys := memtable.data.Keys()
	values := memtable.data.Values()

	for i := range keys {
		deepCopy.data.Put(keys[i], values[i])
	}

	return deepCopy
}

func (ds *DiskStore) Close() error {
	// TODO finish implementing
	return errors.Join(
		ds.writeAheadLog.file.Close(),
	)
}
