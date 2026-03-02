package store

import (
	"fmt"

	rbt "github.com/emirpasic/gods/trees/redblacktree"

	"github.com/tferdous17/genesis/utils"
)

type Memtable struct {
	nodeId      string
	data        *rbt.Tree
	sizeInBytes uint32
}

func NewMemtable(nodeId string) *Memtable {
	return &Memtable{
		nodeId:      nodeId,
		data:        rbt.NewWithStringComparator(),
		sizeInBytes: 0,
	}
}

func (m *Memtable) Put(key string, value *Record) {
	m.data.Put(key, value)
	// ? Possibly size inflation on duplicate keys... handle another time
	m.sizeInBytes += value.RecordSize
}

func (m *Memtable) Get(key string) (*Record, error) {
	val, found := m.data.Get(key)
	if !found {
		return nil, utils.ErrKeyNotFound
	}
	return val.(*Record), nil
}

func (m *Memtable) GetAllKVPairs() map[string]*Record {
	kvPairs := make(map[string]*Record, m.data.Size())

	iter := m.data.Iterator()
	for iter.Next() {
		kvPairs[iter.Key().(string)] = iter.Value().(*Record)
	}

	return kvPairs
}

func (m *Memtable) PrintAllRecords() {
	fmt.Println(m.returnAllRecordsInSortedOrder())
}

func (m *Memtable) Flush(directory string) (*SSTable, error) {
	sortedEntries := m.returnAllRecordsInSortedOrder()
	table, err := InitSSTableOnDisk(m.nodeId, directory, sortedEntries)
	if err != nil {
		return nil, fmt.Errorf("flush memtable to disk: %w", err)
	}

	return table, nil
}

func (m *Memtable) returnAllRecordsInSortedOrder() []*Record {
	records := make([]*Record, 0, m.data.Size())
	it := m.data.Iterator()
	for it.Next() {
		records = append(records, it.Value().(*Record))
	}
	return records
}

func (m *Memtable) clear() {
	// clear memtable once flushed to SSTable
	m.data.Clear()
	m.sizeInBytes = 0
}

func castToRecordSlice(interfaceSlice *[]interface{}) []Record {
	recordSlice := make([]Record, len(*interfaceSlice))
	for i, iface := range *interfaceSlice {
		record, ok := iface.(Record)
		if !ok {
			_ = fmt.Errorf("element %d is not a Record", i)
		}
		recordSlice[i] = record
	}
	return recordSlice
}
