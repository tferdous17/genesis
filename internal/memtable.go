package internal

type Memtable struct {
	data RedBlackTree
}

func NewMemtable() *Memtable {
	return &Memtable{RedBlackTree{root: nil}}
}

func (m *Memtable) Put(key string, value KeyEntry) {
	m.data.Insert(key, value)
}

func (m *Memtable) Get(key string) (KeyEntry, error) {
	return m.data.Find(key)
}
