package store

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"

	"github.com/tferdous17/genesis/utils"
)

const (
	DataFileExtension  string = ".data"
	IndexFileExtension string = ".index"
	BloomFileExtension string = ".bloom"

	SparseIndexSampleSize int = 1000
)

var sstTableCounter uint32

type SSTable struct {
	dataFile    *os.File
	indexFile   *os.File
	bloomFilter *BloomFilter
	sstCounter  uint32
	minKey      string
	maxKey      string
	sizeInBytes uint32
	sparseKeys  []sparseIndex
}

// InitSSTableOnDisk directory to store sstable, (sorted) entries to store in said table
func InitSSTableOnDisk(directory string, entries *[]Record) (*SSTable, error) {
	atomic.AddUint32(&sstTableCounter, 1)
	table := &SSTable{
		sstCounter: sstTableCounter,
	}
	err := table.InitTableFiles(directory)
	if err != nil {
		return nil, err
	}
	err2 := writeEntriesToSST(entries, table)
	if err2 != nil {
		return nil, err
	}

	return table, nil
}

func (sst *SSTable) InitTableFiles(directory string) error {
	// Create "storage" folder with read-write-execute for owner & group, read-only for others
	if err := os.MkdirAll("../storage", 0755); err != nil {
		return err
	}

	// create data and index files
	dataFile, err := os.Create(getNextSstFilename(directory, sst.sstCounter) + DataFileExtension)

	if err != nil {
		return fmt.Errorf("failed to create data file: %w", err)
	}

	indexFile, err := os.Create(getNextSstFilename(directory, sst.sstCounter) + IndexFileExtension)

	if err != nil {
		err := dataFile.Close()
		if err != nil {
			return err
		} // Clean up previously created files
		return fmt.Errorf("failed to create index file: %w", err)
	}

	bloomFile, err := os.Create(getNextSstFilename(directory, sst.sstCounter) + BloomFileExtension)

	if err != nil {
		err := dataFile.Close()
		if err != nil {
			return err
		} // Clean up previously created files
		err2 := indexFile.Close()
		if err2 != nil {
			return err2
		}
		return fmt.Errorf("failed to create bloom filter file: %w", err)
	}

	sst.dataFile, sst.indexFile = dataFile, indexFile
	sst.bloomFilter = NewBloomFilter(bloomFile)

	return nil
}

func getNextSstFilename(directory string, sstCounter uint32) string {
	return fmt.Sprintf("../%s/sst_%d", directory, sstCounter)
}

type sparseIndex struct {
	keySize    uint32
	key        string
	byteOffset uint32 // where to start reading from
}

func writeEntriesToSST(sortedEntries *[]Record, table *SSTable) error {
	buf := new(bytes.Buffer)
	var byteOffsetCounter uint32

	// Keep track of min, max for searching in the case our desired key is outside these bounds
	table.minKey = (*sortedEntries)[0].Key
	table.maxKey = (*sortedEntries)[len(*sortedEntries)-1].Key

	// * every 1000th key will be put into the sparse index
	for i := range *sortedEntries {
		table.sizeInBytes += (*sortedEntries)[i].RecordSize
		if i%SparseIndexSampleSize == 0 {
			table.sparseKeys = append(table.sparseKeys, sparseIndex{
				keySize:    (*sortedEntries)[i].Header.KeySize,
				key:        (*sortedEntries)[i].Key,
				byteOffset: byteOffsetCounter,
			})
		}
		byteOffsetCounter += (*sortedEntries)[i].RecordSize
		err := (*sortedEntries)[i].EncodeKV(buf)
		if err != nil {
			return err
		}
	}

	// after encoding all entries, dump into the SSTable
	if err := utils.WriteToFile(buf.Bytes(), table.dataFile); err != nil {
		fmt.Println("write to sst err:", err)
	}
	// * Set up sparse index
	utils.Logf("SPARSE KEYS: %v", table.sparseKeys)
	err := populateSparseIndexFile(&table.sparseKeys, table.indexFile)
	if err != nil {
		return err
	}

	// * Set up + populate bloom filter
	table.bloomFilter.InitBloomFilterAttrs(uint32(len(*sortedEntries)))
	populateBloomFilter(sortedEntries, table.bloomFilter)

	return nil
}

func populateSparseIndexFile(indices *[]sparseIndex, indexFile *os.File) error {
	// encode and write to index file
	buf := new(bytes.Buffer)
	for i := range *indices {
		err := binary.Write(buf, binary.LittleEndian, (*indices)[i].keySize)
		if err != nil {
			return err
		}
		buf.WriteString((*indices)[i].key)
		err2 := binary.Write(buf, binary.LittleEndian, (*indices)[i].byteOffset)
		if err2 != nil {
			return err2
		}
	}

	if err := utils.WriteToFile(buf.Bytes(), indexFile); err != nil {
		fmt.Println("write to indexfile err:", err)
	}
	return nil

}

func populateBloomFilter(entries *[]Record, bloomFilter *BloomFilter) {
	for i := range *entries {
		err := bloomFilter.Add((*entries)[i].Key)
		if err != nil {
			return
		}
	}

	bfBytes := make([]byte, bloomFilter.bitSetSize)
	for i, b := range bloomFilter.bitSet {
		if b {
			bfBytes[i] = 1
		} else {
			bfBytes[i] = 0
		}
	}
	if err := utils.WriteToFile(bfBytes, bloomFilter.file); err != nil {
		fmt.Println("write to bloomfile err:", err)
	}
}

func (sst *SSTable) Get(key string) (string, error) {
	if key < sst.minKey || key > sst.maxKey {
		return "", utils.ErrKeyNotWithinTable
	}

	if !sst.bloomFilter.MightContain(key) {
		utils.LogRED("BLOOM FILTER: %s is not a member of this table", key)
		return "", utils.ErrKeyNotWithinTable
	}

	// * Seek to the best candidate offset from the sparse index
	startOffset := int64(sst.sparseKeys[sst.getCandidateByteOffsetIndex(key)].byteOffset)
	if _, err := sst.dataFile.Seek(startOffset, io.SeekStart); err != nil {
		return "", fmt.Errorf("seek to sparse index offset: %w", err)
	}

	// Use a buffered reader from the seek point to avoid syscalls per read due to io.ReadFull on the raw *os.File
	reader := bufio.NewReader(sst.dataFile)

	headerBuf := make([]byte, 17)
	for {
		// Read header
		_, err := io.ReadFull(reader, headerBuf)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return "", utils.ErrKeyNotFound
			}
			return "", fmt.Errorf("read header: %w", err)
		}

		h := &Header{}
		if err := h.DecodeHeader(headerBuf); err != nil {
			return "", fmt.Errorf("decode header: %w", err)
		}

		// Read in the key-value after the header (cursor naturally moves)
		kvBuf := make([]byte, h.KeySize+h.ValueSize)
		if _, err := io.ReadFull(reader, kvBuf); err != nil {
			return "", fmt.Errorf("read key-value: %w", err)
		}

		// Append the header and kv together in order to decode as a whole
		r := &Record{}
		if err := r.DecodeKV(append(headerBuf, kvBuf...)); err != nil {
			return "", fmt.Errorf("decode record: %w", err)
		}

		if r.Key == key {
			utils.LogGREEN("FOUND KEY %s -> VALUE %s\n", key, r.Value)
			return r.Value, nil
		} else if r.Key > key {
			// * return early
			// * this works b/c since our data is sorted, if the curr key is > target key,
			// * ..then the key is not in this table
			return "", utils.ErrKeyNotWithinTable
		} // else continue the loop if r.Key < key.
	}
}

// Looks through the sparse indexes and determines which byte offset to start from when scanning the SSTable
func (sst *SSTable) getCandidateByteOffsetIndex(targetKey string) int {
	low := 0
	high := len(sst.sparseKeys) - 1

	for low <= high {
		mid := (low + high) / 2

		cmp := strings.Compare(targetKey, sst.sparseKeys[mid].key)
		if cmp > 0 { // targetKey > sparseKeys[mid]
			low = mid + 1
		} else if cmp < 0 { // targetKey < sparseKeys[mid]
			high = mid - 1
		} else { // equal
			return mid
		}
	}
	utils.LogCYAN("CANDIDATE BYTE OFFSET: %d AT INDEX %d", sst.sparseKeys[low-1].byteOffset, uint32(low-1))
	return low - 1
}
