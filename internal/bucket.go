package internal

import (
	"bitcask-go/utils"
	"container/heap"
	"errors"
	"fmt"
	"io"
)

type Bucket struct {
	minTableSize  uint32
	avgBucketSize uint32
	bucketLow     float32
	bucketHigh    float32
	tables        []SSTable
}

const DefaultTableSizeInBytes uint32 = 3_000
const MinThreshold = 4
const MaxThreshold = 12

func InitBucket(table *SSTable) *Bucket {
	bucket := &Bucket{
		minTableSize: DefaultTableSizeInBytes,
		bucketLow:    0.5,
		bucketHigh:   1.5,
		tables:       []SSTable{*table},
	}
	bucket.calculateAvgBucketSize()
	return bucket
}

func (b *Bucket) AppendTableToBucket(table *SSTable) {
	if table.sizeInBytes < b.minTableSize {
		return
	}

	lowerSizeThreshold := uint32(b.bucketLow * float32(b.avgBucketSize))   // 50% lower than avg size
	higherSizeThreshold := uint32(b.bucketHigh * float32(b.avgBucketSize)) // 50% higher than avg size

	// calculate low and high thresholds-- this avoids a skewed distribution of SSTable sizes within a given bucket
	if lowerSizeThreshold < table.sizeInBytes && table.sizeInBytes < higherSizeThreshold {
		b.tables = append(b.tables, *table)
	}
	// update avg size on each append
	b.calculateAvgBucketSize()

	b.TriggerCompaction()
}

func (b *Bucket) calculateAvgBucketSize() {
	var sum uint32 = 0
	for i := range b.tables {
		sum += b.tables[i].sizeInBytes
	}
	b.avgBucketSize = sum / uint32(len(b.tables))
}

func (b *Bucket) TriggerCompaction() {
	if len(b.tables) < MinThreshold {
		return
	}

	utils.LogRED("STARTING COMPACTION WITH LENGTH %d", len(b.tables))

	var allSortedRuns [][]Record

	for i := range b.tables {
		var currSortedRun []Record
		var currOffset uint32

		// Set seek 0 to for every table otherwise the seek position will be at the end of each file by default
		// I assume because of previous reading done on said files?
		b.tables[i].dataFile.Seek(int64(currOffset), 0)
		for {
			currEntry := make([]byte, headerSize)
			_, err := io.ReadFull(b.tables[i].dataFile, currEntry)
			if errors.Is(err, io.EOF) {
				utils.Log("END OF FILE")
				break
			}

			h := &Header{}
			h.DecodeHeader(currEntry)

			// * move the cursor so we can read the rest of the record
			currOffset += headerSize // can do this since headerSize is constant
			b.tables[i].dataFile.Seek(int64(currOffset), 0)
			// * set up []byte for the rest of the record
			currRecord := make([]byte, h.KeySize+h.ValueSize)
			if _, err := io.ReadFull(b.tables[i].dataFile, currRecord); err != nil {
				fmt.Println("READFULL ERR:", err)
				break
			}
			// * append both []byte together in order to decode as a whole
			currEntry = append(currEntry, currRecord...) // full size of the record
			r := &Record{}
			r.DecodeKV(currEntry)

			currSortedRun = append(currSortedRun, *r)

			currOffset += r.Header.KeySize + r.Header.ValueSize
			b.tables[i].dataFile.Seek(int64(currOffset), 0)
		}
		allSortedRuns = append(allSortedRuns, currSortedRun)
	}

	// * now we have all our sorted runs
	h := MinRecordHeap{}
	for i := range allSortedRuns {
		filterAndDeleteTombstones(allSortedRuns[i])
		removeOutdatedEntires(allSortedRuns[i])
	}

	for i := range allSortedRuns {
		for j := range allSortedRuns[i] {
			heap.Push(&h, allSortedRuns[i][j])
		}
	}

	// * got all of them into 1 table basically
	// * now we need to handle tombstone values
	// * also need to handle duplicates -> take only most recent timestamp
	utils.LogGREEN("Heap len = %d", h.Len())
	for h.Len() > 0 {
		ele := heap.Pop(&h)
		fmt.Println(ele)
	}

}
