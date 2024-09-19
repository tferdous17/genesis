package main

import (
	"bitcask-go/internal"
	"bytes"
	"fmt"
)

func main() {
	buf := bytes.Buffer{}
	fmt.Println(buf)

	header := internal.Header{TimeStamp: 123, KeySize: 150, ValueSize: 240}
	err := header.EncodeHeader(&buf)
	fmt.Println(err)

	fmt.Println("buf is now", buf)

	buf2 := make([]byte, 12)
	_, err2 := buf.Read(buf2) // it reads the first len(buf2) bytes from buf into buf2
	fmt.Println(err2)

	err3 := header.DecodeHeader(buf2)
	fmt.Println(err3)

	fmt.Println("-----printing header info below-----")
	fmt.Println(header.TimeStamp)
	fmt.Println(header.KeySize)
	fmt.Println(header.ValueSize)

}
