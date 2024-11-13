package store

type Store interface {
	Put(key *string, value *string) error
	Get(key string) (string, error)
	Delete(key string) error
	Close() bool
}
