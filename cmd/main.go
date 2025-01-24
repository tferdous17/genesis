package main

import "github.com/tferdous17/genesis/store"

func main() {
	c := store.NewCluster(5)
	c.Open()
}
