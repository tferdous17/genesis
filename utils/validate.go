package utils

func ValidateKV(key string, value string) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if len(value) == 0 {
		return ErrEmptyValue
	}
	return nil
}
