package store

type Store interface {
	Put(key *string, value *string) error
	Get(key string) (string, error)
	Close() bool
}
