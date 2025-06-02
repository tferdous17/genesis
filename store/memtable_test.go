package store

import (
	"testing"
)

func BenchmarkMemtable_Put(b *testing.B) {
	memtable := NewMemtable()

	for i := 0; i < b.N; i++ {
		key := generateRandomKey()
		record := &Record{
			Header:     Header{},
			Key:        key,
			Value:      "testVal",
			RecordSize: 0,
		}
		memtable.Put(&key, record)
	}

	opsPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}

func BenchmarkMemtable_Get(b *testing.B) {
	memtable := NewMemtable()

	for i := 0; i < 1_000_000; i++ {
		key := generateRandomKey()
		record := &Record{
			Header:     Header{},
			Key:        key,
			Value:      "testVal",
			RecordSize: 0,
		}
		memtable.Put(&key, record)
	}
	testKey := "Foxtrot"
	memtable.Put(&testKey, &Record{})
	b.ResetTimer()

	for i := 0; i < 1_000_000; i++ {
		_, err := memtable.Get(&testKey)
		if err != nil {
			return
		}

	}

	opsPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}
