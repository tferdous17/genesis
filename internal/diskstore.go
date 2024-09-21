package internal

import (
	"errors"
	"fmt"
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
	serverFile    *os.File
	writePosition int
	keyDir        map[string]KeyEntry
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

}
