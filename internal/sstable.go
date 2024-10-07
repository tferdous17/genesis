package internal

import (
	"bitcask-go/utils"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync/atomic"
)

const (
	DataFileExtension  string = ".data"
	IndexFileExtension string = ".index"

	SparseIndexSampleSize int = 100
)

var sstTableCounter uint32

type SSTable struct {
	dataFile   *os.File
	indexFile  *os.File
	sstCounter uint32
	minKey     string
	maxKey     string
}

// InitSSTableOnDisk directory to store sstable, (sorted) entries to store in said table
func InitSSTableOnDisk(directory string, entries []Record) {
	atomic.AddUint32(&sstTableCounter, 1)
	table := &SSTable{
		sstCounter: sstTableCounter,
	}
	table.InitTableFiles(directory)
	writeEntriesToSST(entries, table)
}

func (sst *SSTable) InitTableFiles(directory string) {
	// Create "storage" folder with read-write-execute for owner & group, read-only for others
	if err := os.MkdirAll("../storage", 0755); err != nil {
		fmt.Println("mkdir err", err)
	}

	// create data and index files
	dataFile, _ := os.Create(getNextSstFilename(directory, sst.sstCounter) + DataFileExtension)
	indexFile, err := os.Create(getNextSstFilename(directory, sst.sstCounter) + IndexFileExtension)

	if err != nil {
		fmt.Println("init file err", err)
	}

	sst.dataFile, sst.indexFile = dataFile, indexFile
}

func getNextSstFilename(directory string, sstCounter uint32) string {
	return fmt.Sprintf("../%s/sst_%d", directory, sstCounter)
}

type sparseIndex struct {
	keySize    uint32
	key        string
	byteOffset uint32
}

func writeEntriesToSST(sortedEntries []Record, table *SSTable) {
	buf := new(bytes.Buffer)
	var sparseKeys []sparseIndex
	var byteOffsetCounter uint32

	// Keep track of min, max for searching in the case our desired key is outside these bounds
	table.minKey = sortedEntries[0].Key
	table.maxKey = sortedEntries[len(sortedEntries)-1].Key

	// * every 100th key will be put into the sparse index
	for i := range sortedEntries {
		if i%SparseIndexSampleSize == 0 {
			sparseKeys = append(sparseKeys, sparseIndex{
				keySize:    sortedEntries[i].Header.KeySize,
				key:        sortedEntries[i].Key,
				byteOffset: byteOffsetCounter,
			})
		}
		byteOffsetCounter += sortedEntries[i].RecordSize
		sortedEntries[i].EncodeKV(buf)
	}

	// after encoding all entries, dump into the SSTable
	if err := utils.WriteToFile(buf.Bytes(), table.dataFile); err != nil {
		fmt.Println("write to sst err:", err)
	}
	populateSparseIndex(sparseKeys, table.indexFile)
}

func populateSparseIndex(indices []sparseIndex, indexFile *os.File) {
	// encode and write to index file
	buf := new(bytes.Buffer)
	for i := range indices {
		fmt.Printf("Key: %s | KeySize: %d | ByteOffset: %d |", indices[i].key, indices[i].keySize, indices[i].byteOffset)
		binary.Write(buf, binary.LittleEndian, &indices[i].keySize)
		buf.WriteString(indices[i].key)
		binary.Write(buf, binary.LittleEndian, &indices[i].byteOffset)
	}
	fmt.Println("Sparse Index Bytes:", buf.Bytes())

	if err := utils.WriteToFile(buf.Bytes(), indexFile); err != nil {
		fmt.Println("write to indexfile err:", err)
	}
}
