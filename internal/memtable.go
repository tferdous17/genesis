package internal

type Memtable struct {
	data RedBlackTree
	size int
}

func NewMemtable() *Memtable {
	return &Memtable{RedBlackTree{root: nil}, 0}
}

func (m *Memtable) Put(key string, value Record) {
	m.data.Insert(key, value)
	m.size++
}

func (m *Memtable) Get(key string) (Record, error) {
	return m.data.Find(key)
}

func (m *Memtable) PrintAllRecords() {
}

func (m *Memtable) Flush(filename string) {
	sortedEntries := m.data.ReturnAllRecordsInSortedOrder()
	table := NewSSTable(filename)
	table.writeEntriesToSST(sortedEntries)
	m.clear()
}

func (m *Memtable) clear() {
	// clear table once flushed to SSTable
	m.data.root = nil
	m.size = 0
}
