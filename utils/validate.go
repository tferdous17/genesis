package utils

import "errors"

func ValidateKV(key string, value string) error {
	if len(key) == 0 {
		return errors.New("invalid key: can not be empty")
	}
	if len(value) == 0 {
		return errors.New("invalid value: can not be empty")
	}
	return nil
}
