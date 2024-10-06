package internal

import (
	"bitcask-go/utils"
	"bytes"
	"fmt"
	"os"
	"sync/atomic"
)

const (
	DATA_FILE_EXTENSION  string = ".data"
	INDEX_FILE_EXTENSION string = ".index"
)

var sstTableCounter uint32

type SSTable struct {
	dataFile   *os.File
	indexFile  *os.File
	sstCounter uint32
}

func NewSSTable(filename string) *SSTable {
	dataFile, indexFile := initializeFromDisk(filename)
	atomic.AddUint32(&sstTableCounter, 1)
	return &SSTable{
		dataFile:   dataFile,
		indexFile:  indexFile,
		sstCounter: sstTableCounter,
	}
}

func initializeFromDisk(filename string) (*os.File, *os.File) {
	// create data and index files
	dataFile, err := os.OpenFile(filename+DATA_FILE_EXTENSION, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	indexFile, err := os.OpenFile(filename+INDEX_FILE_EXTENSION, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("init file err", err)
	}
	return dataFile, indexFile
}

func (sst *SSTable) getNextSstFilename(directory string) string {
	return fmt.Sprintf("%s/sst_%d", directory, sst.sstCounter)
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
