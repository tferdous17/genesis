package internal

import (
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

*/

type DiskStore struct {
	Server *os.File
}

func NewDiskStore(serverName string) (DiskStore, error) {
	serverFile, err := os.Create(serverName)
	ds := DiskStore{serverFile}

	if err != nil {
		fmt.Println("error creating disk store", err)
	}
	defer serverFile.Close()

	return ds, err
}

func (ds *DiskStore) Put(key string, value string) {

}

func (ds *DiskStore) Get(key string) string {
	return ""
}
