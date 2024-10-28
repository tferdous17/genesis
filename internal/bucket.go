package internal

import (
	"bitcask-go/utils"
	"cmp"
	"container/heap"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
)

type Bucket struct {
	minTableSize  uint32
	avgBucketSize uint32
	bucketLow     float32
	bucketHigh    float32
	tables        []SSTable
}

const DefaultTableSizeInBytes uint32 = 3_000

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

func InitEmptyBucket() *Bucket {
	bucket := &Bucket{
		minTableSize: DefaultTableSizeInBytes,
		bucketLow:    0.5,
		bucketHigh:   1.5,
		tables:       []SSTable{},
	}
	return bucket
}

func (b *Bucket) AppendTableToBucket(table *SSTable) {
	if table.sizeInBytes < b.minTableSize {
		return
	}

	if len(b.tables) == 0 {
		b.tables = append(b.tables, *table)
		b.calculateAvgBucketSize()
		return
	}

	lowerSizeThreshold := uint32(b.bucketLow * float32(b.avgBucketSize))   // 50% lower than avg size
	higherSizeThreshold := uint32(b.bucketHigh * float32(b.avgBucketSize)) // 50% higher than avg size

	if lowerSizeThreshold <= table.sizeInBytes && table.sizeInBytes <= higherSizeThreshold {
		b.tables = append(b.tables, *table)
	} else {
		utils.Log("Could not append table. Out of range")
	}

	//update avg size on each append
	b.calculateAvgBucketSize()
}

func (b *Bucket) calculateAvgBucketSize() {
	var sum uint32 = 0
	for i := range b.tables {
		sum += b.tables[i].sizeInBytes
	}
	b.avgBucketSize = sum / uint32(len(b.tables))
}

func (b *Bucket) NeedsCompaction(minNumTables, maxNumTables int) bool {
	if len(b.tables) >= minNumTables && len(b.tables) <= maxNumTables {
		return true
	}
	return false
}

func (b *Bucket) TriggerCompaction() *SSTable {
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
				//utils.Log("END OF FILE")
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
		for j := range allSortedRuns[i] {
			heap.Push(&h, allSortedRuns[i][j])
		}
	}

	// * now that they're all in a heap, we need to throw it into 1 big sstable
	utils.LogGREEN("Heap len = %d", h.Len())
	finalSortedRun := make([]Record, 0)
	for h.Len() > 0 {
		ele := heap.Pop(&h)
		finalSortedRun = append(finalSortedRun, ele.(Record))
	}

	filterAndDeleteTombstones(&finalSortedRun)
	removeOutdatedEntires(&finalSortedRun)

	// once the new merged table gets created, we add it to a new bucket
	mergedSSTable := InitSSTableOnDisk("storage", finalSortedRun)

	// ! now we need to delete the old sstables from disk to free up space
	deleteOldSSTables(b.tables)

	return mergedSSTable
}

func filterAndDeleteTombstones(sortedRun *[]Record) {
	var collectedTombstones []string

	// collect all tombstones to delete
	for i := range *sortedRun {
		if (*sortedRun)[i].Header.Tombstone == 1 {
			collectedTombstones = append(collectedTombstones, (*sortedRun)[i].Key)
		}
	}

	// now look at every key in collectedTombstones and delete it from the sorted run
	for i := 0; i < len(*sortedRun); {
		if slices.Contains(collectedTombstones, (*sortedRun)[i].Key) {
			if i < len(*sortedRun)-1 {
				*sortedRun = slices.Delete(*sortedRun, i, i+1)
			} else {
				*sortedRun = (*sortedRun)[:len(*sortedRun)-1]
			}
		} else {
			i++
		}
	}
}

func removeOutdatedEntires(sortedRun *[]Record) {
	// * take every entry -> append to a map, if value for a given map key is > 1,
	// * then sort the value (which will be a slice) & delete all values except the last 1 in the overall slice

	var tempMap = make(map[string][]Record)

	for i := range *sortedRun {
		tempMap[(*sortedRun)[i].Key] = append(tempMap[(*sortedRun)[i].Key], (*sortedRun)[i])
	}

	for _, v := range tempMap {
		if len(v) > 1 {
			slices.SortFunc(v, func(a, b Record) int {
				return cmp.Compare(a.Header.TimeStamp, b.Header.TimeStamp)
			})

			for i := 0; i < len(v)-1; i++ {
				idx := slices.Index(*sortedRun, v[i])
				*sortedRun = slices.Delete(*sortedRun, idx, idx+1)
			}
		}
	}
}

func deleteOldSSTables(tables []SSTable) error {
	for i := range tables {
		files := []string{tables[i].dataFile.Name(), tables[i].indexFile.Name(), tables[i].bloomFilter.file.Name()}

		for _, file := range files {
			if err := os.Remove(file); err != nil {
				utils.Log("DELETION ERROR")
				return err
			}
		}
	}
	tables = []SSTable{} // empty the slice
	return nil
}
