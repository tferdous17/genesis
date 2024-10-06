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

// InitSSTableOnDisk directory to store sstable, (sorted) entries to store in said table
func InitSSTableOnDisk(directory string, entries []Record) {
	atomic.AddUint32(&sstTableCounter, 1)
	table := &SSTable{
		sstCounter: sstTableCounter,
	}
	table.initTableFiles(directory)
	writeEntriesToSST(entries, table.dataFile)
}

func (sst *SSTable) initTableFiles(directory string) {
	// Create "storage" folder with read-write-execute for owner & group, read-only for others
	if err := os.MkdirAll("../storage", 0755); err != nil {
		fmt.Println("mkdir err", err)
	}

	// create data and index files
	dataFile, _ := os.Create(sst.getNextSstFilename(directory) + DATA_FILE_EXTENSION)
	indexFile, err := os.Create(sst.getNextSstFilename(directory) + INDEX_FILE_EXTENSION)

	if err != nil {
		fmt.Println("init file err", err)
	}

	sst.dataFile, sst.indexFile = dataFile, indexFile
}

func (sst *SSTable) getNextSstFilename(directory string) string {
	return fmt.Sprintf("../%s/sst_%d", directory, sst.sstCounter)
}

func writeEntriesToSST(entries []Record, dataFile *os.File) {
	buf := new(bytes.Buffer)
	for i := range entries {
		entries[i].EncodeKV(buf)
	}
	// after encoding each entry, dump into the SSTable
	if err := utils.WriteToFile(buf.Bytes(), dataFile); err != nil {
		fmt.Println("write to sst err:", err)
	}
}
