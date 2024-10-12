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
	memtable      *Memtable
	writeAheadLog *os.File
	levels        [][]SSTable
}

type Operation int

const (
	PUT Operation = iota
	GET
	DELETE
)

func NewDiskStore() (*DiskStore, error) {
	ds := &DiskStore{memtable: NewMemtable()}

	logFile, err := os.OpenFile("genesis_wal.log", os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ds.writeAheadLog = logFile

	return ds, err
}

func (ds *DiskStore) Put(key string, value string) error {
	// This check is to prevent writes occurring while memtable is locked and flushing to disk
	if ds.memtable.locked {
		return utils.ErrMemtableLocked
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
	for i := len(ds.levels[0]) - 1; i >= 0; i-- {
		value, err := ds.levels[0][i].Get(key)
		if errors.Is(err, utils.ErrKeyNotWithinTable) {
			continue
		}
		return value, err
	}
	return "<!not_found>", utils.ErrKeyNotFound
}

// TODO: This entire method will need to be reworked w/ RBTrees and SSTables
func (ds *DiskStore) Delete(key string) error {
	return nil
}

func (ds *DiskStore) ListOfAllKeys() {
	ds.memtable.PrintAllRecords()
}

// ? Flush in separate goroutine?
var counter int = 0

func (ds *DiskStore) FlushMemtable() {
	if ds.memtable.sizeInBytes >= 6500 {
		counter++
		utils.Logf("SIZE AT TIME OF FLUSHING (#%d): %d\n", counter, int(ds.memtable.sizeInBytes))
		sstable := ds.memtable.Flush("storage")
		// ! levels is empty. so we cant append to a nonexistent index
		if len(ds.levels) == 0 {
			ds.levels = append(ds.levels, []SSTable{*sstable})
		} else {
			ds.levels[0] = append(ds.levels[0], *sstable)
		}
	}
}

func (ds *DiskStore) DebugMemtable() {
	ds.memtable.PrintAllRecords()
	utils.Logf("CURRENT SIZE IN BYTES: %d", ds.memtable.sizeInBytes)
}
