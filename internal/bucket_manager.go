package internal

import (
	"bitcask-go/utils"
)

type BucketManager struct {
	buckets           map[int]*Bucket // maybe make map?
	highestLvl        int
	minTableThreshold int
	maxTableThreshold int
}

// InitBucketManager Initializes manager + first level of buckets
func InitBucketManager() *BucketManager {
	manager := &BucketManager{
		buckets:           make(map[int]*Bucket),
		highestLvl:        0,
		minTableThreshold: 4,
		maxTableThreshold: 12,
	}
	manager.buckets[0] = InitEmptyBucket()

	return manager
}

func (bm *BucketManager) InsertTable(table *SSTable) {
	var lvlToAppend int
	for currLvl, bucket := range bm.buckets {
		lvlToAppend = currLvl + calculateLevel(*bucket, table)
		utils.LogCYAN("lvl=%d", lvlToAppend)
		utils.LogGREEN("k+lvl = %d", lvlToAppend)
		_, ok := bm.buckets[lvlToAppend]
		if !ok {
			utils.Log("CREATING NEW LEVEL")
			bm.buckets[lvlToAppend] = InitEmptyBucket()
			bm.highestLvl++
		}
		bm.buckets[lvlToAppend].AppendTableToBucket(table)
		break
	}

	utils.LogGREEN("lvl to append = %d", lvlToAppend)
	if bm.shouldCompact(lvlToAppend) {
		// trigger compact
		bm.compact(lvlToAppend)
	}
}

func (bm *BucketManager) DebugBM() {
	utils.Log("Length of each bucket:")
	for k, v := range bm.buckets {
		utils.LogCYAN("Level = %d, Len of level = %d", k, len(v.tables))
	}
}

func (bm *BucketManager) compact(level int) {
	bkt := bm.buckets[level]
	//ONLY triggers if threshold is reached in the bucket
	mergedTable := bkt.TriggerCompaction()

	if mergedTable != nil {
		// Take this table and throw it into a new level
		bm.InsertTable(mergedTable)
	}
}

func (bm *BucketManager) shouldCompact(level int) bool {
	return bm.buckets[level].NeedsCompaction(bm.minTableThreshold, bm.maxTableThreshold)
}

func calculateLevel(bucket Bucket, table *SSTable) int {
	if table.sizeInBytes < bucket.minTableSize {
		return -1
	}

	lowerSizeThreshold := uint32(bucket.bucketLow * float32(bucket.avgBucketSize))   // 50% lower than avg size
	higherSizeThreshold := uint32(bucket.bucketHigh * float32(bucket.avgBucketSize)) // 50% higher than avg size

	if table.sizeInBytes < lowerSizeThreshold {
		return -1
	} else if table.sizeInBytes > higherSizeThreshold {
		return 1
	} else {
		return 0
	}
}
