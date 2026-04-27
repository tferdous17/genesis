package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/tferdous17/genesis/utils"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// setupTestEnv chdirs into a subdirectory of a temp root so that
// getNextSstFilename's hardcoded "../<dir>" prefix resolves to a real path.
// Returns the plain directory name to pass to InitSSTableOnDisk ("storage").
// The original working directory is restored automatically via t.Cleanup.
func setupTestEnv(t *testing.T) string {
	t.Helper()

	root := t.TempDir()

	inner := filepath.Join(root, "work")
	if err := os.MkdirAll(inner, 0755); err != nil {
		t.Fatalf("MkdirAll work dir: %v", err)
	}

	orig, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	if err := os.Chdir(inner); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })

	// SSTable files land at root/storage == ../storage relative to inner.
	return "storage"
}

// makeRecord builds a minimal *Record suitable for SSTable writes.
func makeRecord(key, value string) *Record {
	h := Header{
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
	}
	return &Record{
		Header:     h,
		Key:        key,
		Value:      value,
		RecordSize: uint32(headerSize) + uint32(len(key)) + uint32(len(value)),
	}
}

// sortedRecords returns *Record slice sorted ascending by key.
func sortedRecords(pairs [][2]string) []*Record {
	sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })
	records := make([]*Record, len(pairs))
	for i, p := range pairs {
		records[i] = makeRecord(p[0], p[1])
	}
	return records
}

// newSST creates an SSTable in a properly configured temp environment and
// registers t.Cleanup to close it.
func newSST(t *testing.T, pairs [][2]string) *SSTable {
	t.Helper()
	dir := setupTestEnv(t)
	records := sortedRecords(pairs)
	sst, err := InitSSTableOnDisk("testnode", dir, records)
	if err != nil {
		t.Fatalf("InitSSTableOnDisk: %v", err)
	}
	t.Cleanup(func() { _ = sst.Close() })
	return sst
}

// isNotFoundErr returns true for any error signalling a missing/out-of-range key.
func isNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, utils.ErrKeyNotFound) ||
		errors.Is(err, utils.ErrKeyNotWithinTable)
}

// ---------------------------------------------------------------------------
// InitSSTableOnDisk
// ---------------------------------------------------------------------------

func TestInitSSTableOnDisk_CreatesFiles(t *testing.T) {
	dir := setupTestEnv(t)
	records := sortedRecords([][2]string{{"alpha", "1"}, {"beta", "2"}})

	sst, err := InitSSTableOnDisk("node1", dir, records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer sst.Close()

	for _, ext := range []string{DataFileExtension, IndexFileExtension, BloomFileExtension} {
		pattern := fmt.Sprintf("../storage/node1_sst_*%s", ext)
		matches, _ := filepath.Glob(pattern)
		if len(matches) == 0 {
			t.Errorf("expected file with extension %s to exist", ext)
		}
	}
}

func TestInitSSTableOnDisk_SetsMinMaxKeys(t *testing.T) {
	sst := newSST(t, [][2]string{{"cherry", "c"}, {"apple", "a"}, {"banana", "b"}})

	if sst.minKey != "apple" {
		t.Errorf("minKey = %q, want %q", sst.minKey, "apple")
	}
	if sst.maxKey != "cherry" {
		t.Errorf("maxKey = %q, want %q", sst.maxKey, "cherry")
	}
}

func TestInitSSTableOnDisk_PopulatesSparseIndex(t *testing.T) {
	pairs := make([][2]string, SparseIndexSampleSize*2+1)
	for i := range pairs {
		pairs[i] = [2]string{fmt.Sprintf("key%06d", i), fmt.Sprintf("val%d", i)}
	}
	sst := newSST(t, pairs)

	if len(sst.sparseKeys) < 2 {
		t.Errorf("expected >=2 sparse index entries for %d records, got %d",
			len(pairs), len(sst.sparseKeys))
	}
}

func TestInitSSTableOnDisk_SingleRecord(t *testing.T) {
	sst := newSST(t, [][2]string{{"onlyone", "value"}})

	if sst.minKey != "onlyone" || sst.maxKey != "onlyone" {
		t.Errorf("single-record table: minKey=%q maxKey=%q", sst.minKey, sst.maxKey)
	}
}

func TestInitSSTableOnDisk_AccumulatesSizeInBytes(t *testing.T) {
	sst := newSST(t, [][2]string{{"k1", "v1"}, {"k2", "v2"}})
	if sst.sizeInBytes == 0 {
		t.Error("sizeInBytes should be > 0 after writing records")
	}
}

// ---------------------------------------------------------------------------
// SSTable.Get - happy path
// ---------------------------------------------------------------------------

func TestGet_ExistingKey(t *testing.T) {
	sst := newSST(t, [][2]string{{"apple", "fruit"}, {"banana", "yellow"}, {"cherry", "red"}})

	val, err := sst.Get("banana")
	if err != nil {
		t.Fatalf("Get(banana): unexpected error: %v", err)
	}
	if val != "yellow" {
		t.Errorf("Get(banana) = %q, want %q", val, "yellow")
	}
}

func TestGet_FirstKey(t *testing.T) {
	sst := newSST(t, [][2]string{{"aardvark", "first"}, {"zebra", "last"}})
	val, err := sst.Get("aardvark")
	if err != nil {
		t.Fatalf("Get(aardvark): %v", err)
	}
	if val != "first" {
		t.Errorf("Get(aardvark) = %q, want %q", val, "first")
	}
}

func TestGet_LastKey(t *testing.T) {
	sst := newSST(t, [][2]string{{"aardvark", "first"}, {"zebra", "last"}})
	val, err := sst.Get("zebra")
	if err != nil {
		t.Fatalf("Get(zebra): %v", err)
	}
	if val != "last" {
		t.Errorf("Get(zebra) = %q, want %q", val, "last")
	}
}

func TestGet_SingleRecord(t *testing.T) {
	sst := newSST(t, [][2]string{{"solo", "single"}})
	val, err := sst.Get("solo")
	if err != nil {
		t.Fatalf("Get(solo): %v", err)
	}
	if val != "single" {
		t.Errorf("Get(solo) = %q, want %q", val, "single")
	}
}

func TestGet_LargeTable_AllKeysRetrievable(t *testing.T) {
	const n = SparseIndexSampleSize*3 + 7
	pairs := make([][2]string, n)
	for i := range pairs {
		pairs[i] = [2]string{fmt.Sprintf("key%06d", i), fmt.Sprintf("value%d", i)}
	}
	sst := newSST(t, pairs)

	sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })
	for _, idx := range []int{0, 500, 1000, 1500, n - 1} {
		k, want := pairs[idx][0], pairs[idx][1]
		got, err := sst.Get(k)
		if err != nil {
			t.Errorf("Get(%q): %v", k, err)
			continue
		}
		if got != want {
			t.Errorf("Get(%q) = %q, want %q", k, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// SSTable.Get - error / boundary cases
// ---------------------------------------------------------------------------

func TestGet_KeyBelowMin_ReturnsError(t *testing.T) {
	sst := newSST(t, [][2]string{{"banana", "b"}, {"cherry", "c"}})
	_, err := sst.Get("aaa")
	if !isNotFoundErr(err) {
		t.Errorf("expected key-not-found error for key below minKey, got %v", err)
	}
}

func TestGet_KeyAboveMax_ReturnsError(t *testing.T) {
	sst := newSST(t, [][2]string{{"banana", "b"}, {"cherry", "c"}})
	_, err := sst.Get("zzz")
	if !isNotFoundErr(err) {
		t.Errorf("expected key-not-found error for key above maxKey, got %v", err)
	}
}

func TestGet_KeyInRange_ButMissing(t *testing.T) {
	sst := newSST(t, [][2]string{{"alpha", "a"}, {"delta", "d"}})
	_, err := sst.Get("bravo")
	if err == nil {
		t.Error("expected error for missing in-range key, got nil")
	}
}

func TestGet_EmptyKey(t *testing.T) {
	sst := newSST(t, [][2]string{{"alpha", "a"}, {"beta", "b"}})
	_, err := sst.Get("")
	if err == nil {
		t.Error("expected error for empty key, got nil")
	}
}

// ---------------------------------------------------------------------------
// SSTable.getCandidateByteOffsetIndex
// ---------------------------------------------------------------------------

func TestGetCandidateByteOffsetIndex_ExactMatch(t *testing.T) {
	sst := &SSTable{
		sparseKeys: []*sparseIndex{
			{key: "a", byteOffset: 0},
			{key: "m", byteOffset: 100},
			{key: "z", byteOffset: 200},
		},
	}
	if idx := sst.getCandidateByteOffsetIndex("m"); idx != 1 {
		t.Errorf("exact match: expected index 1, got %d", idx)
	}
}

func TestGetCandidateByteOffsetIndex_BetweenEntries(t *testing.T) {
	sst := &SSTable{
		sparseKeys: []*sparseIndex{
			{key: "a", byteOffset: 0},
			{key: "m", byteOffset: 100},
			{key: "z", byteOffset: 200},
		},
	}
	if idx := sst.getCandidateByteOffsetIndex("g"); idx != 0 {
		t.Errorf("between a/m: expected index 0, got %d", idx)
	}
}

func TestGetCandidateByteOffsetIndex_BeyondLastEntry(t *testing.T) {
	sst := &SSTable{
		sparseKeys: []*sparseIndex{
			{key: "a", byteOffset: 0},
			{key: "m", byteOffset: 100},
		},
	}
	if idx := sst.getCandidateByteOffsetIndex("r"); idx != 1 {
		t.Errorf("beyond last entry: expected index 1, got %d", idx)
	}
}

func TestGetCandidateByteOffsetIndex_BeforeFirstEntry(t *testing.T) {
	sst := &SSTable{
		sparseKeys: []*sparseIndex{
			{key: "m", byteOffset: 0},
			{key: "z", byteOffset: 100},
		},
	}
	if idx := sst.getCandidateByteOffsetIndex("a"); idx != 0 {
		t.Errorf("before first entry: expected index 0, got %d", idx)
	}
}

func TestGetCandidateByteOffsetIndex_SingleEntry(t *testing.T) {
	sst := &SSTable{
		sparseKeys: []*sparseIndex{
			{key: "only", byteOffset: 42},
		},
	}
	for _, key := range []string{"aaa", "only", "zzz"} {
		if idx := sst.getCandidateByteOffsetIndex(key); idx != 0 {
			t.Errorf("single sparse entry: key %q -> index %d, want 0", key, idx)
		}
	}
}

// ---------------------------------------------------------------------------
// SSTable.Close
// ---------------------------------------------------------------------------

func TestClose_CanBeCalledOnce(t *testing.T) {
	sst := newSST(t, [][2]string{{"k", "v"}})
	if err := sst.Close(); err != nil {
		t.Errorf("Close() unexpected error: %v", err)
	}
	// t.Cleanup will call Close() a second time; errors there are ignored.
}

func TestClose_FilesAreActuallyClosed(t *testing.T) {
	dir := setupTestEnv(t)
	records := sortedRecords([][2]string{{"a", "1"}, {"b", "2"}})
	sst, err := InitSSTableOnDisk("node1", dir, records)
	if err != nil {
		t.Fatalf("InitSSTableOnDisk: %v", err)
	}
	if err := sst.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	buf := make([]byte, 1)
	if _, err := sst.dataFile.Read(buf); err == nil {
		t.Error("expected read on closed dataFile to fail")
	}
}

// ---------------------------------------------------------------------------
// Bloom filter integration
// ---------------------------------------------------------------------------

func TestGet_BloomFilterRejectionForOutOfRangeKey(t *testing.T) {
	sst := newSST(t, [][2]string{
		{"key000001", "v1"},
		{"key000003", "v3"},
	})
	_, err := sst.Get("key000002") // in range but absent
	if err == nil {
		t.Error("expected error for absent in-range key, got nil")
	}
}

// ---------------------------------------------------------------------------
// Counter monotonicity
// ---------------------------------------------------------------------------

func TestSSTTableCounter_Monotonic(t *testing.T) {
	dir := setupTestEnv(t)
	records := sortedRecords([][2]string{{"x", "y"}})

	sst1, err := InitSSTableOnDisk("n", dir, records)
	if err != nil {
		t.Fatalf("InitSSTableOnDisk sst1: %v", err)
	}
	defer sst1.Close()

	sst2, err := InitSSTableOnDisk("n", dir, records)
	if err != nil {
		t.Fatalf("InitSSTableOnDisk sst2: %v", err)
	}
	defer sst2.Close()

	if sst2.sstCounter <= sst1.sstCounter {
		t.Errorf("counter should be strictly increasing: sst1=%d sst2=%d",
			sst1.sstCounter, sst2.sstCounter)
	}
}

// ---------------------------------------------------------------------------
// Parallel / race safety
// ---------------------------------------------------------------------------

func TestGet_ConcurrentReads(t *testing.T) {
	const n = 512 // divisible by 8
	pairs := make([][2]string, n)
	for i := range pairs {
		pairs[i] = [2]string{fmt.Sprintf("ckey%04d", i), fmt.Sprintf("val%d", i)}
	}
	sst := newSST(t, pairs)
	sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })

	done := make(chan struct{}, 8)
	for i := 0; i < 8; i++ {
		go func(chunk [][2]string) {
			defer func() { done <- struct{}{} }()
			for _, p := range chunk {
				_, _ = sst.Get(p[0])
			}
		}(pairs[i*n/8 : (i+1)*n/8])
	}
	for i := 0; i < 8; i++ {
		<-done
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkSSTable_Get(b *testing.B) {
	const n = 10_000
	pairs := make([][2]string, n)
	for i := range pairs {
		pairs[i] = [2]string{fmt.Sprintf("benchkey%08d", i), fmt.Sprintf("val%d", i)}
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })
	records := make([]*Record, n)
	for i, p := range pairs {
		records[i] = makeRecord(p[0], p[1])
	}

	root := b.TempDir()
	inner := filepath.Join(root, "work")
	_ = os.MkdirAll(inner, 0755)
	orig, _ := os.Getwd()
	_ = os.Chdir(inner)
	b.Cleanup(func() { _ = os.Chdir(orig) })

	sst, err := InitSSTableOnDisk("bench", "storage", records)
	if err != nil {
		b.Fatalf("InitSSTableOnDisk: %v", err)
	}
	defer sst.Close()

	target := pairs[n/2][0]
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sst.Get(target)
	}
}

func BenchmarkSSTable_Init(b *testing.B) {
	const n = 1_000
	pairs := make([][2]string, n)
	for i := range pairs {
		pairs[i] = [2]string{fmt.Sprintf("initkey%08d", i), fmt.Sprintf("val%d", i)}
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })
	records := make([]*Record, n)
	for i, p := range pairs {
		records[i] = makeRecord(p[0], p[1])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		root := b.TempDir()
		inner := filepath.Join(root, "work")
		_ = os.MkdirAll(inner, 0755)
		orig, _ := os.Getwd()
		_ = os.Chdir(inner)

		sst, err := InitSSTableOnDisk("bench", "storage", records)
		if err != nil {
			_ = os.Chdir(orig)
			b.Fatalf("InitSSTableOnDisk: %v", err)
		}
		_ = sst.Close()
		_ = os.Chdir(orig)
	}
}
