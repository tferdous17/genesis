package store

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// wanted actual numbers for how much the bloom filter and compaction each
// save on a Get(), instead of just assuming. these benchmarks try to isolate
// one variable at a time so the two don't get muddled together. nothing here
// touches production code, its all test-only setup.
// ---------------------------------------------------------------------------

func chdirIntoTempWorkDir(b *testing.B) {
	b.Helper()
	root := b.TempDir()
	inner := filepath.Join(root, "work")
	if err := os.MkdirAll(inner, 0755); err != nil {
		b.Fatalf("MkdirAll: %v", err)
	}
	orig, err := os.Getwd()
	if err != nil {
		b.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(inner); err != nil {
		b.Fatalf("Chdir: %v", err)
	}
	b.Cleanup(func() { _ = os.Chdir(orig) })
}

const (
	// 12 = BucketManager's maxTableThreshold, i.e. the most un-compacted
	// tables a bucket would realistically ever hold before compacting
	impactNumTables       = 12
	impactRecordsPerTable = 2000
)

// keyVal interleaves keys across tables (always even) so every table's
// range overlaps with the others -- if each table got its own clean band
// instead, the min/max check would reject most tables on its own and the
// bloom filter would never actually get exercised
func keyVal(tableIdx, recordIdx int) int {
	return (recordIdx*impactNumTables + tableIdx) * 2
}

func keyStr(val int) string {
	return fmt.Sprintf("key%010d", val)
}

// buildOverlappingTables creates a bucket's worth of SSTables with
// overlapping key ranges, basically what a bucket looks like right before
// compaction runs
func buildOverlappingTables(b *testing.B) []*SSTable {
	b.Helper()
	tables := make([]*SSTable, 0, impactNumTables)
	for t := 0; t < impactNumTables; t++ {
		records := make([]*Record, impactRecordsPerTable)
		for i := 0; i < impactRecordsPerTable; i++ {
			records[i] = makeRecord(keyStr(keyVal(t, i)), "val")
		}
		sst, err := InitSSTableOnDisk("implbench", "storage", records)
		if err != nil {
			b.Fatalf("InitSSTableOnDisk: %v", err)
		}
		tables = append(tables, sst)
	}
	return tables
}

func closeAll(tables []*SSTable) {
	for _, t := range tables {
		_ = t.Close()
	}
}

// disableBloomFilter flips every bit to true so MightContain can never say
// no -- basically simulates not having a bloom filter at all, since every
// table now has to go through the sparse index + scan to get ruled out
func disableBloomFilter(tables []*SSTable) {
	for _, sst := range tables {
		for i := range sst.bloomFilter.bitSet {
			sst.bloomFilter.bitSet[i] = true
		}
	}
}

// ---------------------------------------------------------------------------
// bloom filter isolation: target key doesn't exist anywhere, but it's picked
// (odd value, everything real is even) so it still falls inside every
// table's min/max range. that way the bounds check can't bail early on its
// own -- the bloom filter is the only thing that can actually reject a table.
// ---------------------------------------------------------------------------

func BenchmarkGet_MultiTable_WithBloomFilter(b *testing.B) {
	chdirIntoTempWorkDir(b)
	tables := buildOverlappingTables(b)
	defer closeAll(tables)

	target := keyStr(59) // odd -> guaranteed absent, but within every table's bounds

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, t := range tables {
			_, _ = t.Get(target)
		}
	}
}

func BenchmarkGet_MultiTable_WithoutBloomFilter(b *testing.B) {
	chdirIntoTempWorkDir(b)
	tables := buildOverlappingTables(b)
	defer closeAll(tables)
	disableBloomFilter(tables)

	target := keyStr(59)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, t := range tables {
			_, _ = t.Get(target)
		}
	}
}

// ---------------------------------------------------------------------------
// compaction isolation: target key is real this time, but it only lives in
// the last table checked -- worst case for the uncompacted version, which
// has to strike out on every other table first (bloom filter working
// normally throughout). compacted version just finds it in one lookup.
// ---------------------------------------------------------------------------

func BenchmarkGet_Uncompacted_ManyOverlappingTables(b *testing.B) {
	chdirIntoTempWorkDir(b)
	tables := buildOverlappingTables(b)
	defer closeAll(tables)

	// Key only present in the last table (table index impactNumTables-1, record 0)
	target := keyStr(keyVal(impactNumTables-1, 0))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, t := range tables {
			if v, err := t.Get(target); err == nil {
				_ = v
				break
			}
		}
	}
}

// ---------------------------------------------------------------------------
// both together now -- worst case (bloom filter disabled, tables still
// spread across the max a bucket would ever hold) against best case (bloom
// filter on, already compacted down to one table).
// ---------------------------------------------------------------------------

func BenchmarkGet_Worst_NoBloomNoCompaction(b *testing.B) {
	chdirIntoTempWorkDir(b)
	tables := buildOverlappingTables(b)
	defer closeAll(tables)
	disableBloomFilter(tables)

	target := keyStr(keyVal(impactNumTables-1, 0)) // present, only in last table

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, t := range tables {
			if v, err := t.Get(target); err == nil {
				_ = v
				break
			}
		}
	}
}

func BenchmarkGet_Best_BloomAndCompaction(b *testing.B) {
	chdirIntoTempWorkDir(b)
	tables := buildOverlappingTables(b)

	bucket := &Bucket{tables: tables}
	merged, err := bucket.TriggerCompaction()
	if err != nil {
		b.Fatalf("TriggerCompaction: %v", err)
	}
	defer merged.Close()

	target := keyStr(keyVal(impactNumTables-1, 0))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = merged.Get(target)
	}
}

func BenchmarkGet_Compacted_SingleMergedTable(b *testing.B) {
	chdirIntoTempWorkDir(b)
	tables := buildOverlappingTables(b)

	bucket := &Bucket{tables: tables}
	merged, err := bucket.TriggerCompaction()
	if err != nil {
		b.Fatalf("TriggerCompaction: %v", err)
	}
	defer merged.Close()

	target := keyStr(keyVal(impactNumTables-1, 0))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = merged.Get(target)
	}
}
