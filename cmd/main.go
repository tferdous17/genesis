package main

import "genesis/store"

func main() {
	c := store.NewDiskStoreDistributed(5)
	c.Open()
}
