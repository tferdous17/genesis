package store

import (
	"fmt"
	"github.com/serialx/hashring"
)

type Node struct {
	ID    string // probably server address or something?
	Store *DiskStore
}

type Cluster struct {
	hashRing *hashring.HashRing
	nodes    map[string]*Node
}

var nodeCount = 1

func (c *Cluster) initNodes(numOfNodes int) {
	c.nodes = make(map[string]*Node)
	var nodeAddrs []string

	for i := 0; i < numOfNodes; i++ {
		store, _ := NewDiskStore()
		node := Node{
			ID:    fmt.Sprintf("node-%d", nodeCount),
			Store: store,
		}
		c.nodes[node.ID] = &node
		nodeCount++
		nodeAddrs = append(nodeAddrs, node.ID)
	}

	c.hashRing = hashring.New(nodeAddrs)
}

func (c *Cluster) Put(key, value string) {
	nodeAddr, _ := c.hashRing.GetNode(key) // get which node this key should be on
	node, ok := c.nodes[nodeAddr]

	if ok {
		node.Store.Put(&key, &value)
	}
}

func (c *Cluster) Get(key string) (string, error) {
	nodeAddr, _ := c.hashRing.GetNode(key) // get which node this key should be on
	node, ok := c.nodes[nodeAddr]

	if ok {
		fmt.Println("key found at " + nodeAddr)
		return node.Store.Get(key)
	}

	return "", nil
}

func (c *Cluster) Delete(key string) error {
	nodeAddr, _ := c.hashRing.GetNode(key) // get which node this key should be on
	node, ok := c.nodes[nodeAddr]

	if ok {
		fmt.Println("key deleted at " + nodeAddr)
		return node.Store.Delete(key)
	}

	return nil
}

func (c *Cluster) PrintDiagnostics() {
	for k, v := range c.nodes {
		fmt.Printf(k + " num keys: ")
		v.Store.LengthOfMemtable()
	}
}
