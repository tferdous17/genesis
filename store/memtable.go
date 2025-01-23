package store

import (
	"fmt"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/tferdous17/genesis/utils"
)

type Memtable struct {
	data        *rbt.Tree
	sizeInBytes uint32
}

func NewMemtable() *Memtable {
	return &Memtable{
		rbt.NewWithStringComparator(),
		0,
	}
}

func (m *Memtable) Put(key *string, value *Record) {
	m.data.Put(*key, *value)
	m.sizeInBytes += value.RecordSize
}

func (m *Memtable) Get(key *string) (Record, error) {
	val, found := m.data.Get(*key)
	if !found {
		return Record{}, utils.ErrKeyNotFound
	}
	return val.(Record), nil
}

func (m *Memtable) GetAllKVPairs() map[string]Record {
	kvPairs := make(map[string]Record)

	for _, k := range m.data.Keys() {
		val, _ := m.data.Get(k)
		kvPairs[k.(string)] = val.(Record)
	}

	return kvPairs
}

func (m *Memtable) PrintAllRecords() {
	fmt.Println(m.returnAllRecordsInSortedOrder())
}

func (m *Memtable) Flush(directory string) *SSTable {
	sortedEntries := m.returnAllRecordsInSortedOrder()
	table := InitSSTableOnDisk(directory, castToRecordSlice(&sortedEntries))

	return table
}

func (m *Memtable) returnAllRecordsInSortedOrder() []interface{} {
	data := inorderRBT(m.data.Root, make([]interface{}, 0))
	return data
}

func inorderRBT(node *rbt.Node, data []interface{}) []interface{} {
	if node != nil {
		data = inorderRBT(node.Left, data)
		data = append(data, node.Value)
		data = inorderRBT(node.Right, data)
	}
	return data
}

func (m *Memtable) clear() {
	// clear memtable once flushed to SSTable
	m.data.Clear()
	m.sizeInBytes = 0
}

func castToRecordSlice(interfaceSlice *[]interface{}) *[]Record {
	recordSlice := make([]Record, len(*interfaceSlice))
	for i, iface := range *interfaceSlice {
		record, ok := iface.(Record)
		if !ok {
			fmt.Errorf("element %d is not a Record", i)
		}
		recordSlice[i] = record
	}
	return &recordSlice
}
