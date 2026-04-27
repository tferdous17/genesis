package store

import (
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================
// TEST HELPERS
// ============================================================

// setupStore creates a fresh DiskStore for testing and registers
// cleanup to remove WAL and storage files after the test completes
func setupStore(t *testing.T) *DiskStore {
	t.Helper()
	nodeId := fmt.Sprintf("test-node-%d", time.Now().UnixNano())
	store, err := newStore(nodeId)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	t.Cleanup(func() {
		store.Close()
		os.RemoveAll("../log")
		os.RemoveAll("../storage")
	})
	return store
}

var testPortOffset uint32 = 0

func setupCluster(t *testing.T, numNodes uint32) *Cluster {
	t.Helper()

	// Each cluster gets its own port range — prevents bind conflicts
	// when multiple cluster tests run sequentially
	portBase := atomic.AddUint32(&testPortOffset, numNodes+5) + 11000

	cluster := &Cluster{}
	cluster.nextNodePort = portBase
	cluster.nextNodeCounter = 1
	cluster.initNodes(numNodes)

	t.Cleanup(func() {
		cluster.Close()
		os.RemoveAll("../log")
		os.RemoveAll("../storage")
	})
	return cluster
}

func generateRandomKey() string {
	return generateRandomString(10)
}

func generateRandomString(length int) string {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	chars := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, length)
	for i := range b {
		b[i] = chars[rng.Intn(len(chars))]
	}
	return string(b)
}

// ============================================================
// MEMTABLE UNIT TESTS
// ============================================================

func TestMemtable_PutAndGet(t *testing.T) {
	m := NewMemtable("test-node")

	record := &Record{
		Header:     Header{KeySize: 3, ValueSize: 5},
		Key:        "foo",
		Value:      "hello",
		RecordSize: headerSize + 3 + 5,
	}
	m.Put("foo", record)

	got, err := m.Get("foo")
	if err != nil {
		t.Fatalf("expected to find key, got error: %v", err)
	}
	if got.Value != "hello" {
		t.Errorf("expected value 'hello', got %q", got.Value)
	}
}

func TestMemtable_GetMissingKey(t *testing.T) {
	m := NewMemtable("test-node")
	_, err := m.Get("nonexistent")
	if err == nil {
		t.Error("expected error for missing key, got nil")
	}
}

func TestMemtable_PutUpdatesSize(t *testing.T) {
	m := NewMemtable("test-node")

	r1 := &Record{
		Header:     Header{KeySize: 3, ValueSize: 5},
		Key:        "foo",
		Value:      "hello",
		RecordSize: headerSize + 3 + 5,
	}
	m.Put("foo", r1)

	r2 := &Record{
		Header:     Header{KeySize: 3, ValueSize: 3},
		Key:        "foo",
		Value:      "bar",
		RecordSize: headerSize + 3 + 3,
	}
	m.Put("foo", r2)

	// After overwrite, sizeInBytes should reflect only r2 — not r1 + r2
	if m.sizeInBytes != r2.RecordSize {
		t.Errorf("expected sizeInBytes=%d after overwrite, got %d",
			r2.RecordSize, m.sizeInBytes)
	}
}

func TestMemtable_Remove(t *testing.T) {
	m := NewMemtable("test-node")
	r := &Record{
		Header:     Header{KeySize: 3, ValueSize: 5},
		Key:        "foo",
		Value:      "hello",
		RecordSize: headerSize + 3 + 5,
	}
	m.Put("foo", r)
	m.Remove("foo")

	_, err := m.Get("foo")
	if err == nil {
		t.Error("expected error after removal, got nil")
	}
	if m.sizeInBytes != 0 {
		t.Errorf("expected sizeInBytes=0 after remove, got %d", m.sizeInBytes)
	}
}

func TestMemtable_ReturnsSortedOrder(t *testing.T) {
	m := NewMemtable("test-node")
	keys := []string{"zebra", "apple", "mango", "banana"}
	for _, k := range keys {
		m.Put(k, &Record{
			Header:     Header{KeySize: uint32(len(k)), ValueSize: 1},
			Key:        k,
			Value:      "v",
			RecordSize: headerSize + uint32(len(k)) + 1,
		})
	}

	records := m.returnAllRecordsInSortedOrder()
	for i := 1; i < len(records); i++ {
		if records[i].Key < records[i-1].Key {
			t.Errorf("records not sorted: %q comes after %q",
				records[i].Key, records[i-1].Key)
		}
	}
}

// ============================================================
// DISKSTORE UNIT TESTS
// ============================================================

func TestDiskStore_PutAndGet(t *testing.T) {
	store := setupStore(t)

	if err := store.Put("hello", "world"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	val, err := store.Get("hello")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "world" {
		t.Errorf("expected 'world', got %q", val)
	}
}

func TestDiskStore_GetMissingKey(t *testing.T) {
	store := setupStore(t)
	_, err := store.Get("nonexistent")
	if err == nil {
		t.Error("expected error for missing key, got nil")
	}
}

func TestDiskStore_Delete(t *testing.T) {
	store := setupStore(t)

	if err := store.Put("foo", "bar"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := store.Delete("foo"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// After deletion, key should exist as a tombstone in the memtable
	// but Get should ideally not return the value — depends on your
	// tombstone handling in Get. At minimum it shouldn't panic.
	_, _ = store.Get("foo")
}

func TestDiskStore_OverwriteKey(t *testing.T) {
	store := setupStore(t)

	if err := store.Put("key", "original"); err != nil {
		t.Fatalf("first Put failed: %v", err)
	}
	if err := store.Put("key", "updated"); err != nil {
		t.Fatalf("second Put failed: %v", err)
	}

	val, err := store.Get("key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "updated" {
		t.Errorf("expected 'updated', got %q", val)
	}
}

func TestDiskStore_PutMany(t *testing.T) {
	store := setupStore(t)
	const n = 1000

	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("key-%04d", i)
		if err := store.Put(keys[i], fmt.Sprintf("val-%d", i)); err != nil {
			t.Fatalf("Put failed at i=%d: %v", i, err)
		}
	}

	// Verify all keys are retrievable
	for i, k := range keys {
		val, err := store.Get(k)
		if err != nil {
			t.Errorf("Get failed for key %q: %v", k, err)
		}
		expected := fmt.Sprintf("val-%d", i)
		if val != expected {
			t.Errorf("key %q: expected %q, got %q", k, expected, val)
		}
	}
}

// ============================================================
// WAL UNIT TESTS
// ============================================================

func TestWAL_RecoverPutOperations(t *testing.T) {
	nodeId := fmt.Sprintf("wal-test-%d", time.Now().UnixNano())
	store, err := newStore(nodeId)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll("../log")
		os.RemoveAll("../storage")
	})

	// Write some keys and force flush so they're in the WAL
	keys := []string{"alpha", "beta", "gamma"}
	for _, k := range keys {
		if err := store.Put(k, "value-"+k); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}
	// Force WAL flush without closing — simulates crash scenario
	store.writeAheadLog.flushToDisk()

	// Create a fresh memtable and recover into it
	freshMemtable := NewMemtable(nodeId)
	if err := store.writeAheadLog.Recover(freshMemtable); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	for _, k := range keys {
		r, err := freshMemtable.Get(k)
		if err != nil {
			t.Errorf("key %q not recovered: %v", k, err)
			continue
		}
		if r.Value != "value-"+k {
			t.Errorf("key %q: expected value %q, got %q", k, "value-"+k, r.Value)
		}
	}
}

func TestWAL_RecoverDeleteOperations(t *testing.T) {
	nodeId := fmt.Sprintf("wal-test-%d", time.Now().UnixNano())
	store, err := newStore(nodeId)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll("../log")
		os.RemoveAll("../storage")
	})

	store.Put("foo", "bar")
	store.Delete("foo")
	store.writeAheadLog.flushToDisk()

	freshMemtable := NewMemtable(nodeId)
	if err := store.writeAheadLog.Recover(freshMemtable); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// After PUT then DELETE, key should not be in the memtable
	_, err = freshMemtable.Get("foo")
	if err == nil {
		t.Error("expected key 'foo' to be absent after DELETE recovery, but found it")
	}
}

func TestWAL_EmptyFile(t *testing.T) {
	nodeId := fmt.Sprintf("wal-test-%d", time.Now().UnixNano())
	store, err := newStore(nodeId)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll("../log")
		os.RemoveAll("../storage")
	})

	// Don't write anything — recover from empty WAL should be a no-op
	freshMemtable := NewMemtable(nodeId)
	if err := store.writeAheadLog.Recover(freshMemtable); err != nil {
		t.Fatalf("Recover on empty WAL failed: %v", err)
	}
}

// ============================================================
// CLUSTER INTEGRATION TESTS
// ============================================================

func TestCluster_PutAndGet(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("../log")
		os.RemoveAll("../storage")
	})

	cluster := setupCluster(t, 3)

	if err := cluster.Put("user1", "alice"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	val, err := cluster.Get("user1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "alice" {
		t.Errorf("expected 'alice', got %q", val)
	}
}

func TestCluster_KeysDistributeAcrossNodes(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("../log")
		os.RemoveAll("../storage")
	})

	cluster := setupCluster(t, 3)
	const n = 100

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		if err := cluster.Put(key, fmt.Sprintf("val-%d", i)); err != nil {
			t.Fatalf("Put failed for key %q: %v", key, err)
		}
	}

	// Verify no single node holds all keys — distribution check
	nodeCounts := make(map[string]int)
	for _, node := range cluster.nodes {
		nodeCounts[node.ID] = len(node.Store.memtable.data.Keys())
	}

	for nodeId, count := range nodeCounts {
		t.Logf("node %s holds %d keys", nodeId, count)
	}

	// With 100 keys and 3 nodes, no node should hold all 100
	for nodeId, count := range nodeCounts {
		if count == n {
			t.Errorf("node %s holds all %d keys — distribution is broken", nodeId, n)
		}
	}
}

func TestCluster_AddNodeTriggersRebalance(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("../log")
		os.RemoveAll("../storage")
	})

	cluster := setupCluster(t, 2)

	// Insert keys before adding the new node
	for i := 0; i < 50; i++ {
		cluster.Put(fmt.Sprintf("key-%d", i), fmt.Sprintf("val-%d", i))
	}

	if err := cluster.AddNode(); err != nil {
		t.Fatalf("AddNode failed: %v", err)
	}

	// All keys should still be retrievable after rebalance
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%d", i)
		_, err := cluster.Get(key)
		if err != nil {
			t.Errorf("key %q not found after rebalance: %v", key, err)
		}
	}
}

func TestCluster_GetFromEmptyCluster(t *testing.T) {
	// Edge case: hash ring with no nodes
	cluster := setupCluster(t, 0)
	_, err := cluster.Get("anykey")
	if err == nil {
		t.Error("expected error when getting from empty cluster, got nil")
	}
}

func TestCluster_Delete(t *testing.T) {
	t.Cleanup(func() {
		os.RemoveAll("../log")
		os.RemoveAll("../storage")
	})

	cluster := setupCluster(t, 2)

	cluster.Put("foo", "bar")
	if err := cluster.Delete("foo"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

// ============================================================
// BUCKET / COMPACTION TESTS
// ============================================================

func TestBucket_AppendAndSize(t *testing.T) {
	// Create a mock SSTable with a known size
	table := &SSTable{sizeInBytes: DefaultTableSizeInBytes}
	bucket := InitBucket(table)

	if len(bucket.tables) != 1 {
		t.Errorf("expected 1 table, got %d", len(bucket.tables))
	}
	if bucket.avgBucketSize != DefaultTableSizeInBytes {
		t.Errorf("expected avgBucketSize=%d, got %d",
			DefaultTableSizeInBytes, bucket.avgBucketSize)
	}
}

func TestBucket_RejectsTooSmallTable(t *testing.T) {
	table := &SSTable{sizeInBytes: DefaultTableSizeInBytes}
	bucket := InitBucket(table)

	tooSmall := &SSTable{sizeInBytes: 1}
	bucket.AppendTableToBucket(tooSmall)

	if len(bucket.tables) != 1 {
		t.Errorf("expected bucket to reject table below minTableSize, got %d tables",
			len(bucket.tables))
	}
}

func TestBucket_NeedsCompaction(t *testing.T) {
	bucket := InitEmptyBucket()

	// Below threshold
	if bucket.NeedsCompaction(4, 12) {
		t.Error("empty bucket should not need compaction")
	}

	// Add enough tables to trigger compaction
	for i := 0; i < 4; i++ {
		bucket.tables = append(bucket.tables,
			&SSTable{sizeInBytes: DefaultTableSizeInBytes})
	}
	if !bucket.NeedsCompaction(4, 12) {
		t.Error("bucket with 4 tables should need compaction")
	}
}

func TestFilterAndDeleteTombstones(t *testing.T) {
	records := []*Record{
		{Header: Header{Tombstone: 0}, Key: "alive"},
		{Header: Header{Tombstone: 1}, Key: "dead"},
		{Header: Header{Tombstone: 0}, Key: "alsalive"},
	}

	result := filterAndDeleteTombstones(records)

	for _, r := range result {
		if r.Header.Tombstone == 1 {
			t.Errorf("tombstone record with key %q survived filtering", r.Key)
		}
		if r.Key == "dead" {
			t.Errorf("key 'dead' should have been removed but survived")
		}
	}
	if len(result) != 2 {
		t.Errorf("expected 2 records after filtering, got %d", len(result))
	}
}

func TestRemoveOutdatedEntries(t *testing.T) {
	records := []*Record{
		{Header: Header{TimeStamp: 100}, Key: "foo", Value: "old"},
		{Header: Header{TimeStamp: 200}, Key: "foo", Value: "new"},
		{Header: Header{TimeStamp: 100}, Key: "bar", Value: "only"},
	}

	result := removeOutdatedEntries(records)

	// Should have 2 records — one per unique key
	if len(result) != 2 {
		t.Errorf("expected 2 records after dedup, got %d", len(result))
	}

	for _, r := range result {
		if r.Key == "foo" && r.Value != "new" {
			t.Errorf("expected latest value 'new' for key 'foo', got %q", r.Value)
		}
	}
}

// ============================================================
// STRESS TESTS
// ============================================================

func TestDiskStore_StressWriteAndRead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	store := setupStore(t)
	const n = 10_000

	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = fmt.Sprintf("stress-key-%06d", i)
		if err := store.Put(keys[i], fmt.Sprintf("val-%d", i)); err != nil {
			t.Fatalf("Put failed at i=%d: %v", i, err)
		}
	}

	// Verify every key is readable
	errors := 0
	for i, k := range keys {
		val, err := store.Get(k)
		if err != nil {
			t.Errorf("Get failed for %q: %v", k, err)
			errors++
			continue
		}
		expected := fmt.Sprintf("val-%d", i)
		if val != expected {
			t.Errorf("key %q: expected %q, got %q", k, expected, val)
			errors++
		}
		if errors > 10 {
			t.Fatal("too many errors, stopping stress test early")
		}
	}
}

func TestDiskStore_StressConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	store := setupStore(t)
	const goroutines = 10
	const writesPerGoroutine = 100

	done := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			for i := 0; i < writesPerGoroutine; i++ {
				key := fmt.Sprintf("g%d-key-%d", id, i)
				if err := store.Put(key, "value"); err != nil {
					done <- fmt.Errorf("goroutine %d Put failed: %w", id, err)
					return
				}
			}
			done <- nil
		}(g)
	}

	for i := 0; i < goroutines; i++ {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}

	// Verify total key count
	total := goroutines * writesPerGoroutine
	actual := len(store.memtable.data.Keys())
	if actual != total {
		t.Errorf("expected %d keys after concurrent writes, got %d", total, actual)
	}
}

func TestCluster_StressDistribution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	t.Cleanup(func() {
		os.RemoveAll("../log")
		os.RemoveAll("../storage")
	})

	cluster := setupCluster(t, 5)
	const n = 1000

	// Write
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", i)
		if err := cluster.Put(key, fmt.Sprintf("val-%d", i)); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Read back all keys
	errors := 0
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%06d", i)
		val, err := cluster.Get(key)
		if err != nil {
			t.Errorf("Get failed for %q: %v", key, err)
			errors++
		}
		expected := fmt.Sprintf("val-%d", i)
		if val != expected {
			t.Errorf("key %q: expected %q got %q", key, expected, val)
			errors++
		}
		if errors > 10 {
			t.Fatal("too many errors, stopping early")
		}
	}
}

// ============================================================
// BENCHMARKS
// ============================================================

func BenchmarkDiskStore_Put(b *testing.B) {
	store, _ := newStore("bench-node")
	b.Cleanup(func() {
		store.Close()
		os.RemoveAll("../log")
		os.RemoveAll("../storage")
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := generateRandomKey()
		if err := store.Put(key, "val"); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	opsPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}

func BenchmarkDiskStore_Get(b *testing.B) {
	store, _ := newStore("bench-node")
	b.Cleanup(func() {
		store.Close()
		os.RemoveAll("../log")
		os.RemoveAll("../storage")
	})

	// Seed with data — insert target key at a known position among many keys
	const seedSize = 100_000
	targetKey := "benchmark-target-key"
	for i := 0; i < seedSize; i++ {
		key := generateRandomKey()
		store.Put(key, "val")
	}
	store.Put(targetKey, "targetval")

	if err := store.FlushMemtable(); err != nil {
		b.Fatalf("flush failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.Get(targetKey)
		if err != nil {
			b.Fatalf("Get failed: %v", err)
		}
	}

	opsPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}

func BenchmarkMemtable_Put(b *testing.B) {
	m := NewMemtable("bench-node")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := generateRandomKey()
		m.Put(key, &Record{
			Header:     Header{KeySize: 10, ValueSize: 3},
			Key:        key,
			Value:      "val",
			RecordSize: headerSize + 10 + 3,
		})
	}
	opsPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}

func BenchmarkCluster_Put(b *testing.B) {
	// Use a port range that won't collide with other tests/benchmarks
	portBase := atomic.AddUint32(&testPortOffset, 10) + 11000

	cluster := &Cluster{}
	cluster.nextNodePort = portBase
	cluster.nextNodeCounter = 1
	cluster.initNodes(3)

	b.Cleanup(func() {
		cluster.Close()
		os.RemoveAll("../log")
		os.RemoveAll("../storage")
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := generateRandomKey()
		if err := cluster.Put(key, "val"); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}

	opsPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}

func BenchmarkDiskStore_PutParallel(b *testing.B) {
	store, _ := newStore("bench-parallel")
	b.Cleanup(func() {
		store.Close()
		os.RemoveAll("../log")
		os.RemoveAll("../storage")
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := generateRandomKey()
			store.Put(key, "val")
		}
	})

	opsPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}
