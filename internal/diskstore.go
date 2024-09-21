package internal

import (
	"errors"
	"fmt"
	"io"
	"os"
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
	// in the file to start reading from
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
		err := ds.initKeyDir(fileName)
		if err != nil {
			fmt.Println("error initializing keydir", err)
		}
	}

	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	ds.serverFile = file

	return ds, err
}

func (ds *DiskStore) Put(key string, value string) {
	// create new record for this key, val entry
}

func (ds *DiskStore) Get(key string) string {
	return ""
}

func (ds *DiskStore) Close() bool {
	return false
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
