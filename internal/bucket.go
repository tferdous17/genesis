package internal

import (
	"bitcask-go/utils"
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
}

func (b *Bucket) calculateAvgBucketSize() {
	var sum uint32 = 0
	for i := range b.tables {
		sum += b.tables[i].sizeInBytes
	}
	b.avgBucketSize = sum / uint32(len(b.tables))
}

func (b *Bucket) TriggerCompaction() {
	if len(b.tables) >= MinThreshold {
		utils.LogRED("STARTING COMPACTION WITH LENGTH %d", len(b.tables))
	}

}
