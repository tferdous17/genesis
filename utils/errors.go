package utils

import "errors"

var (
	ErrEmptyKey     = errors.New("invalid key: key can not be empty")
	ErrDuplicateKey = errors.New("invalid key: already in store")
	ErrKeyNotFound  = errors.New("invalid key: not found or deleted")

	ErrEmptyValue = errors.New("invalid value: value can not be empty")

	ErrFileInit = errors.New("error initializing file")

	ErrEncodingHeaderFailed = errors.New("encoding fail: failed to encode header")
	ErrDecodingHeaderFailed = errors.New("decoding fail: failed to decode header")

	ErrEncodingKVFailed = errors.New("encoding fail: failed to encode kv")
	ErrDecodingKVFailed = errors.New("decoding fail: failed to decode kv")

	ErrMemtableLocked = errors.New("memtable fail: currently locked for further operations")

	ErrKeyNotWithinTable = errors.New("sstable: key not within table's range")
)
