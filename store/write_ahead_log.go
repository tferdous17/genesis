package store

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/tferdous17/genesis/utils"
)

const WALBatchThreshold = 1024 * 1024 * 3

// writeAheadLog maintains the log and batches operations to minimize disk writes
type writeAheadLog struct {
	file     *os.File
	opsBatch bytes.Buffer
	mu       sync.Mutex
	stopCh   chan struct{}
}

func newWAL(file *os.File) *writeAheadLog {
	w := &writeAheadLog{
		file:   file,
		stopCh: make(chan struct{}),
	}
	go w.periodicFlush()
	return w
}

// periodicFlush flushes the batch every 500ms regardless of size.
// part of a dual-trigger flush strategy, will flush even if we have infrequent writes that don't hit the threshold
func (w *writeAheadLog) periodicFlush() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			if w.opsBatch.Len() > 0 {
				// Log but don't crash on periodic flush failure,
				// the next tick will retry and the data is still in the buffer
				if err := w.flushToDisk(); err != nil {
					fmt.Printf("WAL periodic flush error: %v\n", err)
				}
			}
			w.mu.Unlock()
		case <-w.stopCh:
			return
		}
	}
}

func (w *writeAheadLog) clearBatch() {
	w.opsBatch.Reset()
}

func (w *writeAheadLog) appendWALOperation(op Operation, record *Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.opsBatch.WriteByte(byte(op))
	if err := record.EncodeKV(&w.opsBatch); err != nil {
		return fmt.Errorf("encode WAL entry: %w", err)
	}

	// size-based flush trigger, the time-based trigger lives in periodicFlush
	if w.opsBatch.Len() >= WALBatchThreshold {
		return w.flushToDisk()
	}

	return nil
}

// flushToDisk writes the current batch to disk and resets the buffer.
// Callers must hold w.mu before calling this.
func (w *writeAheadLog) flushToDisk() error {
	if err := utils.WriteToFile(w.opsBatch.Bytes(), w.file); err != nil {
		return fmt.Errorf("flush WAL to disk: %w", err)
	}
	w.clearBatch()
	return nil
}

// Close flushes any remaining buffered operations, stops the periodic flush
// goroutine, and closes the file handle.
// Without this, any data buffered below the threshold at shutdown time was silently lost
func (w *writeAheadLog) Close() error {
	// stop the periodic flush goroutine before acquiring the lock
	// avoids deadlock if the goroutine is mid-flush when Close is called
	close(w.stopCh)

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.opsBatch.Len() > 0 {
		if err := w.flushToDisk(); err != nil {
			return fmt.Errorf("flush WAL on close: %w", err)
		}
	}
	return w.file.Close()
}

// Recover replays all operations from the WAL file into the memtable.
func (w *writeAheadLog) Recover(memtable *Memtable) error {
	data, err := os.ReadFile(w.file.Name())
	if err != nil {
		return fmt.Errorf("read WAL file: %w", err)
	}

	if len(data) == 0 {
		//log.Printf("WAL is empty, nothing to recover")
		return nil
	}

	log.Printf("WAL recovery started — file size: %d bytes", len(data))

	offset := 0
	recovered := 0
	skipped := 0

	for offset < len(data) {
		if offset >= len(data) {
			break
		}
		op := Operation(data[offset])
		offset++

		if offset+headerSize > len(data) {
			//log.Printf("WAL recovery: partial header at offset %d, stopping", offset)
			skipped++
			break
		}

		headerBuf := data[offset : offset+headerSize]
		h := &Header{}
		if err := h.DecodeHeader(headerBuf); err != nil {
			//log.Printf("WAL recovery: corrupted header at offset %d: %v, stopping", offset, err)
			skipped++
			break
		}

		recordSize := headerSize + int(h.KeySize) + int(h.ValueSize)
		if offset+recordSize > len(data) {
			//log.Printf("WAL recovery: partial record at offset %d (need %d bytes, have %d), stopping",
			//	offset, recordSize, len(data)-offset)
			skipped++
			break
		}

		fullRecord := data[offset : offset+recordSize]
		r := &Record{}
		if err := r.DecodeKV(fullRecord); err != nil {
			//log.Printf("WAL recovery: corrupted record at offset %d: %v, stopping", offset, err)
			skipped++
			break
		}
		offset += recordSize

		switch op {
		case PUT:
			//log.Printf("WAL recovery: replaying PUT key=%q", r.Key)
			memtable.Put(r.Key, r)
			recovered++
		case DELETE:
			//log.Printf("WAL recovery: replaying DELETE key=%q", r.Key)
			memtable.Remove(r.Key)
			recovered++
		default:
			//log.Printf("WAL recovery: unknown operation byte %d at offset %d, skipping", op, offset)
			skipped++
		}
	}

	log.Printf("WAL recovery complete — %d operations replayed, %d skipped", recovered, skipped)
	return nil
}
