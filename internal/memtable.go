package internal

type Memtable struct {
	data        RedBlackTree
	locked      bool
	sizeInBytes uint32
}

func NewMemtable() *Memtable {
	return &Memtable{RedBlackTree{root: nil}, false, 0}
}

func (m *Memtable) Put(key string, value Record) {
	m.data.Insert(key, value)
	m.sizeInBytes += value.RecordSize
}

func (m *Memtable) Get(key string) (Record, error) {
	return m.data.Find(key)
}

func (m *Memtable) PrintAllRecords() {

}

func (m *Memtable) Flush(directory string) *SSTable {
	m.locked = true // lock to prevent operations during flushing process
	sortedEntries := m.data.ReturnAllRecordsInSortedOrder()
	table := InitSSTableOnDisk(directory, sortedEntries)
	m.clear()

	return table
}

func (m *Memtable) clear() {
	// clear memtable once flushed to SSTable
	m.data.root = nil
	m.sizeInBytes = 0
	m.locked = false
}
