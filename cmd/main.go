package main

import "genesis/store"

func main() {
	c := store.NewCluster(5)
	c.Open()
}
