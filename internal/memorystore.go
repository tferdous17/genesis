package internal

type MemoryStore struct {
	data map[string]string
}

func MakeMemoryStore() *MemoryStore {
	return &MemoryStore{
		data: make(map[string]string),
	}
}

func (m *MemoryStore) Get(key string) string {
	return m.data[key]
}

func (m *MemoryStore) Put(key string, value string) {
	m.data[key] = value
}
