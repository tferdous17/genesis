package internal

import (
	"bytes"
	"errors"
	"fmt"
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
	ServerFile *os.File
}

func fileExists(fileName string) bool {
	if _, err := os.Stat(fileName); errors.Is(err, os.ErrNotExist) {
		return false // file does not exist
	}
	return true
}

func NewDiskStore(fileName string) (*DiskStore, error) {

	if fileExists(fileName) {
		// placeholder? not sure how to return an existing diskstore or something
		return nil, nil
	}

	serverFile, err := os.Create(fileName)
	ds := &DiskStore{serverFile}

	if err != nil {
		fmt.Println("error creating disk store", err)
	}
	defer serverFile.Close()

	return ds, err
}

func (ds *DiskStore) Put(key string, value string) {
	record := Record{
		Header: Header{
			TimeStamp: uint32(time.Now().Unix()),
			KeySize:   uint32(len(key)),
			ValueSize: uint32(len(value)),
		},
		Key:        key,
		Value:      value,
		RecordSize: 20,
	}

	buf := bytes.Buffer{}
	err := record.Header.EncodeHeader(&buf)
	err2 := record.EncodeKV(&buf)

	if err != nil || err2 != nil {
		fmt.Println("error encoding header and/or kv", err)
	}

	fmt.Println(buf.Bytes())
	// now lets dump this buffer into our file
	err3 := os.WriteFile("teststore.db", buf.Bytes(), 0644)
	if err3 != nil {
		fmt.Println("error writing to file", err3)
	}
}

func (ds *DiskStore) Get(key string) string {
	return ""
}

func (ds *DiskStore) Close() bool {
	return false
}
