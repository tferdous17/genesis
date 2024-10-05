package internal

type Memtable struct {
	data RedBlackTree
}

func NewMemtable() *Memtable {
	return &Memtable{RedBlackTree{root: nil}}
}

func (m *Memtable) Put(key string, value Record) {
	m.data.Insert(key, value)
}

func (m *Memtable) Get(key string) (Record, error) {
	return m.data.Find(key)
}

func (m *Memtable) PrintAllRecords() {
	m.data.Inorder()
}
