package store

import (
	"genesis/utils"
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
		highestLvl:        1,
		minTableThreshold: 4,
		maxTableThreshold: 12,
	}
	manager.buckets[1] = InitEmptyBucket()

	return manager
}

func (bm *BucketManager) InsertTable(table *SSTable) {
	var levelToAppend = 1

	for currLvl := bm.highestLvl; currLvl > 0; currLvl-- {
		bkt := bm.buckets[currLvl]

		calculatedLevelReturn := calculateLevel(bkt, table)
		levelToAppend = currLvl + calculatedLevelReturn

		if calculatedLevelReturn == -1 {
			continue
		}

		if calculatedLevelReturn == 0 {
			bm.buckets[currLvl].AppendTableToBucket(table)
		} else { // calculatedLevelReturn == 1
			bm.buckets[levelToAppend] = InitEmptyBucket()
			bm.buckets[levelToAppend].AppendTableToBucket(table)
			bm.highestLvl++
		}
		break
	}

	if bm.shouldCompact(levelToAppend) {
		bm.compact(levelToAppend)
	}

	bm.DebugBM()
}

func (bm *BucketManager) RetrieveKey(key *string) (string, error) {
	// start at highest level first
	for lvl := bm.highestLvl; lvl > 0; lvl-- {
		for _, table := range bm.buckets[lvl].tables {
			return table.Get(*key)
		}
	}
	return "<!not_found>", utils.ErrKeyNotFound
}

func (bm *BucketManager) DebugBM() {
	utils.Log("Length of each bucket:")
	for k, v := range bm.buckets {
		utils.LogCYAN("Level = %d, Len of level = %d", k, len(v.tables))
	}
}

func (bm *BucketManager) compact(level int) {
	bkt := bm.buckets[level]
	mergedTable := bkt.TriggerCompaction() // ONLY triggers if threshold is reached in the bucket

	if mergedTable != nil {
		bm.InsertTable(mergedTable)
	}
}

func (bm *BucketManager) shouldCompact(level int) bool {
	return bm.buckets[level].NeedsCompaction(bm.minTableThreshold, bm.maxTableThreshold)
}

func calculateLevel(bucket *Bucket, table *SSTable) int {
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
