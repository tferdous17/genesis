package store

import (
	"bufio"
	"cmp"
	"container/heap"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/tferdous17/genesis/utils"
)

type Bucket struct {
	minTableSize  uint32
	avgBucketSize uint32
	bucketLow     float32
	bucketHigh    float32
	tables        []*SSTable
}

const DefaultTableSizeInBytes uint32 = 3_000

func InitBucket(table *SSTable) *Bucket {
	bucket := &Bucket{
		minTableSize: DefaultTableSizeInBytes,
		bucketLow:    0.5,
		bucketHigh:   1.5,
		tables:       []*SSTable{table},
	}
	bucket.calculateAvgBucketSize()
	return bucket
}

func InitEmptyBucket() *Bucket {
	bucket := &Bucket{
		minTableSize:  DefaultTableSizeInBytes,
		avgBucketSize: DefaultTableSizeInBytes,
		bucketLow:     0.5,
		bucketHigh:    1.5,
		tables:        []*SSTable{},
	}
	return bucket
}

// AdjustSizeThresholdParams bucketLow and bucketHigh determine how close to the avg bucket size an SSTable can be.
// By default, it can be either 50% lower or 50% higher.
func (b *Bucket) AdjustSizeThresholdParams(bucketLow, bucketHigh float32) {
	b.bucketLow = bucketLow
	b.bucketHigh = bucketHigh
}

func (b *Bucket) AppendTableToBucket(table *SSTable) {
	if table.sizeInBytes < b.minTableSize {
		return
	}

	if len(b.tables) == 0 {
		b.tables = append(b.tables, table)
		b.calculateAvgBucketSize()
		return
	}

	lowerSizeThreshold := uint32(b.bucketLow * float32(b.avgBucketSize))   // 50% lower than avg size
	higherSizeThreshold := uint32(b.bucketHigh * float32(b.avgBucketSize)) // 50% higher than avg size

	if lowerSizeThreshold <= table.sizeInBytes && table.sizeInBytes <= higherSizeThreshold {
		b.tables = append(b.tables, table)
	} else {
		utils.Log("Could not append table. Out of range")
	}

	//update avg size on each append
	b.calculateAvgBucketSize()
}

func (b *Bucket) calculateAvgBucketSize() {
	// Prevent divide-by-zero on an empty bucket
	if len(b.tables) == 0 {
		b.avgBucketSize = DefaultTableSizeInBytes
		return
	}

	var sum uint32 = 0
	for i := range b.tables {
		sum += b.tables[i].sizeInBytes
	}
	b.avgBucketSize = sum / uint32(len(b.tables))
}

func (b *Bucket) NeedsCompaction(minNumTables, maxNumTables int) bool {
	return len(b.tables) >= minNumTables && len(b.tables) <= maxNumTables
}

func (b *Bucket) TriggerCompaction() (*SSTable, error) {
	utils.LogGREEN("STARTING COMPACTION WITH LENGTH %d", len(b.tables))

	var allSortedRuns [][]*Record

	for i := range b.tables {
		// Set seek 0 to for every table otherwise the seek position will be at the end of each file by default
		// I assume because of previous reading done on said files?
		if _, err := b.tables[i].dataFile.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek to start of sstable %d: %w", i, err)
		}

		reader := bufio.NewReader(b.tables[i].dataFile)

		var currSortedRun []*Record
		headerBuf := make([]byte, headerSize)

		for {
			if _, err := io.ReadFull(reader, headerBuf); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					break
				}
				return nil, fmt.Errorf("read header from sstable %d: %w", i, err)
			}

			h := &Header{}
			if err := h.DecodeHeader(headerBuf); err != nil {
				return nil, fmt.Errorf("decode header from sstable %d: %w", i, err)
			}

			// Read in the key-value after the header (cursor naturally moves)
			kvBuf := make([]byte, h.KeySize+h.ValueSize)
			if _, err := io.ReadFull(reader, kvBuf); err != nil {
				return nil, fmt.Errorf("read key-value from sstable %d: %w", i, err)
			}

			// Append the header and kv together in order to decode as a whole
			r := &Record{}
			if err := r.DecodeKV(append(headerBuf, kvBuf...)); err != nil {
				return nil, fmt.Errorf("decode record from sstable %d: %w", i, err)
			}

			currSortedRun = append(currSortedRun, r) // store pointer, no dereference
		}
		allSortedRuns = append(allSortedRuns, currSortedRun)
	}

	// * Push all records into the min-heap for merging
	h := MinRecordHeap{}
	for i := range allSortedRuns {
		for j := range allSortedRuns[i] {
			heap.Push(&h, allSortedRuns[i][j])
		}
	}

	// * now that they're all in a heap, we need to throw it into 1 big sstable
	utils.LogGREEN("Heap len = %d", h.Len())
	finalSortedRun := make([]*Record, 0, h.Len())
	for h.Len() > 0 {
		ele := heap.Pop(&h)
		finalSortedRun = append(finalSortedRun, ele.(*Record))
	}

	finalSortedRun = filterAndDeleteTombstones(finalSortedRun)
	finalSortedRun = removeOutdatedEntries(finalSortedRun)

	// once the new merged table gets created, we add it to a new bucket
	mergedSSTable, err := InitSSTableOnDisk(b.tables[0].nodeId, "storage", finalSortedRun)
	if err != nil {
		return nil, fmt.Errorf("create merged sstable: %w", err)
	}

	// ! now we need to delete the old sstables from disk to free up space
	if err := deleteOldSSTables(b.tables); err != nil {
		return nil, fmt.Errorf("delete old sstables: %w", err)
	}
	b.tables = b.tables[:0] // deleteOldSSTables cant do this itself (would've deleted a local copy)

	return mergedSSTable, nil
}

// filterAndDeleteTombstones removes all records whose key appears as a tombstone (1)
// returns the filtered slice
func filterAndDeleteTombstones(sortedRun []*Record) []*Record {
	tombstones := make(map[string]struct{})
	for _, r := range sortedRun {
		if r.Header.Tombstone == 1 {
			tombstones[r.Key] = struct{}{}
		}
	}

	// reuse the backing array to avoid a separate allocation
	// only retain records that are not tombstones and return them via result
	result := sortedRun[:0]
	for _, r := range sortedRun {
		if _, isTombstone := tombstones[r.Key]; !isTombstone {
			result = append(result, r)
		}
	}
	return result
}

func removeOutdatedEntries(sortedRun []*Record) []*Record {
	// * take every entry -> append to a map, if value for a given map key is > 1,
	// * then sort the value (which will be a slice) & delete all values except the last 1 in the overall slice

	var tempMap = make(map[string][]*Record)
	for _, r := range sortedRun {
		tempMap[r.Key] = append(tempMap[r.Key], r)
	}

	for _, v := range tempMap {
		if len(v) > 1 {
			slices.SortFunc(v, func(a, b *Record) int {
				return cmp.Compare(a.Header.TimeStamp, b.Header.TimeStamp)
			})
			// remove all but the most recent entry from the sorted run
			for i := 0; i < len(v)-1; i++ {
				idx := slices.Index(sortedRun, v[i]) // ! O(n), optimize later
				if idx != -1 {
					sortedRun = slices.Delete(sortedRun, idx, idx+1)
				}
			}
		}
	}
	return sortedRun
}

func deleteOldSSTables(tables []*SSTable) error {
	for _, table := range tables {
		files := []string{
			table.dataFile.Name(),
			table.indexFile.Name(),
			table.bloomFilter.file.Name(),
		}

		if err := table.Close(); err != nil {
			return fmt.Errorf("close sstable before deletion: %w", err)
		}

		for _, file := range files {
			if err := os.Remove(file); err != nil {
				return fmt.Errorf("delete sstable file %s: %w", file, err)
			}
		}
	}
	return nil
}
