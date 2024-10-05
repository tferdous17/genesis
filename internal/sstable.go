package internal

import (
	"bitcask-go/utils"
	"bytes"
	"fmt"
	"os"
)

const (
	DATA_FILE_EXTENSION  string = ".data"
	INDEX_FILE_EXTENSION string = ".index"
)

type SSTable struct {
	dataFile  *os.File
	indexFile *os.File
}

func NewSSTable(filename string) *SSTable {
	dataFile, indexFile := initializeFromDisk(filename)
	return &SSTable{
		dataFile:  dataFile,
		indexFile: indexFile,
	}
}

func initializeFromDisk(filename string) (*os.File, *os.File) {
	// create data and index files
	dataFile, err := os.Create(filename + DATA_FILE_EXTENSION)
	indexFile, err := os.Create(filename + INDEX_FILE_EXTENSION)
	if err != nil {
		fmt.Println("init file err", err)
	}
	return dataFile, indexFile
}

func (sst *SSTable) writeEntriesToSST(entries []Record) {
	buf := new(bytes.Buffer)
	for i := range entries {
		entries[i].EncodeKV(buf)
	}
	// after encoding each entry, dump into the SSTable
	if err := utils.WriteToFile(buf.Bytes(), sst.dataFile); err != nil {
		fmt.Println("write to sst err:", err)
	}
}
