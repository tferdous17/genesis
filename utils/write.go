package utils

import "os"

func WriteToFile(data []byte, file *os.File) error {
	// i want to panic on these errors b/c its bad if our data isnt writing
	if _, writeErr := file.Write(data); writeErr != nil {
		panic(writeErr)
	}
	// VERY important to call Sync, b/c this flushes the in-memory buffer of our file to the disk
	// this is what actually makes our data persist as the data is initially stored in said buffer
	// before reaching disk
	if syncErr := file.Sync(); syncErr != nil {
		panic(syncErr)
	}
	return nil
}
