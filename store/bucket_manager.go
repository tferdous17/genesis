package store

import (
	"errors"
	"fmt"
	"github.com/tferdous17/genesis/utils"
)

type BucketManager struct {
	buckets           map[int]*Bucket
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

func (bm *BucketManager) InsertTable(table *SSTable) error {
	levelToAppend := 1
	inserted := false

	for currLvl := bm.highestLvl; currLvl > 0; currLvl-- {
		bkt := bm.buckets[currLvl]

		calculatedLevelReturn := calculateLevel(bkt, table)
		if calculatedLevelReturn == -1 {
			continue
		}

		levelToAppend = currLvl + calculatedLevelReturn

		if calculatedLevelReturn == 0 {
			bm.buckets[currLvl].AppendTableToBucket(table)
		} else { // calculatedLevelReturn == 1
			bm.buckets[levelToAppend] = InitEmptyBucket()
			bm.buckets[levelToAppend].AppendTableToBucket(table)
			bm.highestLvl++
		}
		inserted = true
		break
	}

	if !inserted {
		bm.buckets[1].AppendTableToBucket(table)
		levelToAppend = 1
	}

	if bm.shouldCompact(levelToAppend) {
		err := bm.compact(levelToAppend)
		if err != nil {
			return err
		}
	}

	bm.DebugBM()
	return nil
}

func (bm *BucketManager) RetrieveKey(key string) (string, error) {
	// start at highest level first
	for lvl := bm.highestLvl; lvl > 0; lvl-- {
		for _, table := range bm.buckets[lvl].tables {
			val, err := table.Get(key)
			if err == nil {
				return val, nil
			}

			// key not in this table, so keep searching other tables and levels
			if errors.Is(err, utils.ErrKeyNotWithinTable) || errors.Is(err, utils.ErrKeyNotFound) {
				continue
			}

			// in the case of unexpected errors, surface error immediately
			return "", fmt.Errorf("retrieve key from level %d: %w", lvl, err)
		}
	}
	return "", utils.ErrKeyNotFound
}

func (bm *BucketManager) DebugBM() {
	utils.Log("Length of each bucket:")
	for k, v := range bm.buckets {
		utils.LogCYAN("Level = %d, Len of level = %d", k, len(v.tables))
	}
}

func (bm *BucketManager) compact(level int) error {
	bkt := bm.buckets[level]
	mergedTable, err := bkt.TriggerCompaction() // ONLY triggers if threshold is reached in the bucket
	if err != nil {
		return fmt.Errorf("compaction at level %d: %w", level, err)
	}
	if mergedTable == nil {
		return nil
	}
	if err := bm.InsertTable(mergedTable); err != nil {
		return fmt.Errorf("insert merged table after compaction at level %d: %w", level, err)
	}

	return err
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
