package store

import (
	"bytes"
	"genesis/utils"
	"os"
)

const WALBatchThreshold = 1024 * 1024 * 3

// writeAheadLog maintains the log and batches operations to minimize disk writes
type writeAheadLog struct {
	file     *os.File
	opsBatch []byte
	size     int
}

func (w *writeAheadLog) clearBatch() {
	w.opsBatch = []byte{}
	w.size = 0
}

func (w *writeAheadLog) appendWALOperation(op Operation, record *Record) error {
	buf := new(bytes.Buffer)
	// Store operation as only 1 byte (only WAL entries will have this extra byte)
	buf.WriteByte(byte(op))

	// encode the entire key, value entry
	if encodeErr := record.EncodeKV(buf); encodeErr != nil {
		return utils.ErrEncodingKVFailed
	}

	// store in the batch
	w.opsBatch = append(w.opsBatch, buf.Bytes()...)
	w.size += len(buf.Bytes())

	if w.size >= WALBatchThreshold {
		return w.flushToDisk()
	}

	return nil
}

// Flushes the current batch of operations to disk, only called if size reaches WALBatchThreshold
func (w *writeAheadLog) flushToDisk() error {
	if logErr := utils.WriteToFile(w.opsBatch, w.file); logErr != nil {
		return logErr
	}

	w.clearBatch()
	return nil
}
