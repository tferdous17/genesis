package internal

import (
	"bitcask-go/utils"
	"bytes"
	"fmt"
	//"bytes"
	"errors"
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
	memtable      *Memtable
	writeAheadLog *os.File
}

type Operation int

const (
	PUT Operation = iota
	GET
	DELETE
)

func fileExists(fileName string) bool {
	if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
		return false // file does not exist
	}
	return true
}

func NewDiskStore(fileName string) (*DiskStore, error) {
	ds := &DiskStore{memtable: NewMemtable()}
	//if fileExists(fileName) {
	//	// populate keydir for existing store
	//	err := ds.initKeyDir(fileName)
	//	if err != nil {
	//		return nil, utils.ErrKeyDirInit
	//	}
	//}

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

	// Store record in our memtable
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
	//ds.writePosition += int(record.RecordSize)
	return nil
}

func (ds *DiskStore) Get(key string) (string, error) {
	// look up key in memtable first, if not there-- search the SSTable(s)
	record, err := ds.memtable.Get(key)
	if err != nil {
		// placeholder line, we will actually need to search the SSTables once implemented
		return "", utils.ErrKeyNotFound
	}

	return record.Value, nil
}

// TODO: This entire method will need to be reworked w/ RBTrees and SSTables
//func (ds *DiskStore) Delete(key string) error {
//	//key note: this is an APPEND-ONLY db, so it wouldn't make sense to
//	//overwrite existing data and place a tombstone value there
//	//thus we have to write a semi-copy of the record w/ the tombstone val activated
//
//	_, ok := ds.keyDir[key]
//	if !ok {
//		return utils.ErrKeyNotFound
//	}
//
//	tempVal := ""
//	header := Header{
//		CheckSum:  0,
//		TimeStamp: uint32(time.Now().Unix()),
//		KeySize:   uint32(len(key)),
//		ValueSize: uint32(len(tempVal)),
//	}
//	header.MarkTombstone()
//
//	record := Record{
//		Header:     header,
//		Key:        key,
//		Value:      tempVal,
//		RecordSize: headerSize + header.KeySize + header.ValueSize,
//	}
//	record.Header.CheckSum = record.CalculateChecksum()
//
//	buf := new(bytes.Buffer)
//	if encodeErr := record.EncodeKV(buf); encodeErr != nil {
//		return utils.ErrEncodingKVFailed
//	}
//	ds.writeToFile(buf.Bytes())
//
//	delete(ds.keyDir, key)
//
//	return nil
//}

func (ds *DiskStore) Close() bool {
	// important to actually write to disk thru Sync() first
	ds.serverFile.Sync()
	if err := ds.serverFile.Close(); err != nil {
		return false
	}
	return true
}

// TODO: Entire method will need to be reworked as we will be rebuilding from a write-ahead-log now and not keydir
//func (ds *DiskStore) initKeyDir(existingFile string) error {
//	file, _ := os.Open(existingFile)
//	defer file.Close()
//
//	for {
//		// read 12 bytes from our and store it into header
//		header := make([]byte, headerSize)
//		_, err := io.ReadFull(file, header)
//
//		// error above could be EOF or some other error, so handle either case
//		if err == io.EOF {
//			break
//		}
//		if err != nil {
//			return err
//		}
//		// following func will decode the buffer into a Header{}
//		h, err := NewHeader(header)
//		if err != nil {
//			return err
//		}
//		// read key, val into respective buffers
//		key := make([]byte, h.KeySize)
//		value := make([]byte, h.ValueSize)
//
//		_, keyErr := io.ReadFull(file, key)
//		if keyErr != nil {
//			return err
//		}
//
//		_, valErr := io.ReadFull(file, value)
//		if valErr != nil {
//			return err
//		}
//		// total size of this key, val entry including header
//		totalSize := headerSize + h.KeySize + h.ValueSize
//		ds.keyDir[string(key)] = NewKeyEntry(h.TimeStamp, uint32(ds.writePosition), totalSize)
//		ds.writePosition += int(totalSize)
//	}
//	return nil
//}

func (ds *DiskStore) ListOfAllKeys() {
	ds.memtable.PrintAllRecords()
}

func (ds *DiskStore) FlushMemtable() {
	// ideally we should flush our memtable based on file size (i.e., 1 KB or something at least for testing)
	// start flushing process
	if ds.memtable.sizeInBytes >= 1000 {
		fmt.Println(ds.memtable.sizeInBytes)
		ds.memtable.Flush("storage")
	}
}
