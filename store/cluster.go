package store

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/tferdous17/genesis/http"
	"github.com/tferdous17/genesis/proto"

	"github.com/serialx/hashring"
	"google.golang.org/grpc"
)

type Node struct {
	server *grpc.Server
	ID     string
	Addr   string
	Store  *DiskStore
}

type Cluster struct {
	mu          sync.RWMutex
	hashRing    *hashring.HashRing
	nodes       map[string]*Node
	accumulator *dataMigrationAccumulator

	nextNodeCounter uint32
	nextNodePort    uint32
}

func (c *Cluster) initNodes(numOfNodes uint32) {
	c.nodes = make(map[string]*Node)
	c.accumulator = &dataMigrationAccumulator{}
	c.nextNodeCounter = 1
	c.nextNodePort = 11000

	var nodeAddrs []string

	for i := 0; i < int(numOfNodes); i++ {
		nodeId := fmt.Sprintf("node-%d", c.nextNodeCounter)

		store, err := newStore(nodeId)
		if err != nil {
			log.Printf("failed to create store for node %s: %v", nodeId, err)
			continue
		}

		node := &Node{
			ID:    nodeId,
			Addr:  fmt.Sprintf(":%d", c.nextNodePort),
			Store: store,
		}
		c.nodes[node.Addr] = node

		node.server, err = StartGRPCServer(node.Addr, node)
		if err != nil {
			log.Printf("failed to start gRPC server for node %s: %v", nodeId, err)
			delete(c.nodes, node.Addr)
			continue
		}

		c.nextNodePort++
		c.nextNodeCounter++
		nodeAddrs = append(nodeAddrs, node.Addr)
	}

	c.hashRing = hashring.New(nodeAddrs)
}

func (c *Cluster) AddNode() {
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Println("adding new node @ address", c.nextNodePort)
	nodeId := fmt.Sprintf("node-%d", c.nextNodeCounter)

	store, err := newStore(nodeId)
	if err != nil {
		log.Printf("failed to create store for node %s: %v", nodeId, err)
		return
	}

	node := &Node{
		ID:    nodeId,
		Addr:  fmt.Sprintf(":%d", c.nextNodePort),
		Store: store,
	}
	c.nodes[node.Addr] = node

	node.server, err = StartGRPCServer(node.Addr, node)
	if err != nil {
		log.Printf("failed to start gRPC server for node %s: %v", nodeId, err)
		delete(c.nodes, node.Addr)
		return
	}

	c.nextNodePort++
	c.nextNodeCounter++

	// refresh the hash ring w/ new node
	c.hashRing = c.hashRing.AddNode(node.Addr)
	c.rebalance()
}

func (c *Cluster) RemoveNode(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !strings.HasPrefix(addr, ":") {
		addr = ":" + addr
	}

	node, ok := c.nodes[addr]
	if !ok {
		log.Printf("node @ addr %s not found", addr)
		return
	}

	c.hashRing = c.hashRing.RemoveNode(addr)
	c.rebalance()
	node.server.GracefulStop()
	delete(c.nodes, addr)
	fmt.Printf("node @ addr %s successfully deleted", addr)

}

var defaultPort = ":8080"

func (c *Cluster) Open() {
	clusterService := http.NewClusterService(defaultPort, c)
	if err := clusterService.Start(); err != nil {
		log.Printf("failed to start HTTP server: %v", err)
		return
	}

	fmt.Println("HTTP server started successfully @ port", defaultPort)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh

	// Block until one of the signals above is received

	c.PrintDiagnostics()
	log.Println("signal received, shutting down...")

	if err := clusterService.Close(); err != nil {
		log.Printf("error closing HTTP server: %v", err)
	}

}

func (c *Cluster) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Println("Closing entire cluster..")
	for _, node := range c.nodes {
		node.server.GracefulStop()
	}
}

func (c *Cluster) Put(key, value string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodeAddr, ok := c.hashRing.GetNode(key) // get which node this key should be on
	if !ok {
		return fmt.Errorf("no nodes available in cluster")
	}

	node, ok := c.nodes[nodeAddr]
	if !ok {
		return fmt.Errorf("node not found for addr %s", nodeAddr)
	}

	log.Printf("PUT key=%s node=%s", key, nodeAddr)
	return node.Store.Put(key, value)
}

func (c *Cluster) Get(key string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodeAddr, ok := c.hashRing.GetNode(key) // get which node this key should be on
	if !ok {
		return "", fmt.Errorf("no nodes available in cluster")
	}

	node, ok := c.nodes[nodeAddr]
	if !ok {
		return "", fmt.Errorf("node not found for addr %s", nodeAddr)
	}

	log.Printf("GET key=%s node=%s", key, nodeAddr)
	return node.Store.Get(key)
}

func (c *Cluster) Delete(key string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodeAddr, ok := c.hashRing.GetNode(key) // get which node this key should be on
	if !ok {
		return fmt.Errorf("no nodes available in cluster")
	}

	node, ok := c.nodes[nodeAddr]
	if !ok {
		return fmt.Errorf("node not found for addr %s", nodeAddr)
	}

	log.Printf("DELETE key=%s node=%s", key, nodeAddr)
	return node.Store.Delete(key)
}

func (c *Cluster) PrintDiagnostics() {
	fmt.Println("DIAGNOSTICS:")
	for _, v := range c.nodes {
		fmt.Printf("%s", v.ID+" @ address "+v.Addr+" , num keys: ")
		v.Store.LengthOfMemtable()
	}
}

// dataMigrationAccumulator is meant to keep track of every single group of records that needs to be migrated
// srcNode ":11000" -> destNode ":11000" : []Record{rec1,rec2,...}
type dataMigrationAccumulator struct {
	data map[string]map[string][]*Record
}

func (d *dataMigrationAccumulator) Init(nodeAddresses []string) {
	d.data = make(map[string]map[string][]*Record)
	for _, addr := range nodeAddresses {
		d.data[addr] = make(map[string][]*Record)
	}
}

func (d *dataMigrationAccumulator) Append(srcNode string, destNode string, data *Record) {
	_, ok := d.data[srcNode][destNode]
	if !ok {
		d.data[srcNode][destNode] = make([]*Record, 0)
	}
	d.data[srcNode][destNode] = append(d.data[srcNode][destNode], data)
}

func (d *dataMigrationAccumulator) ClearAccumulator() {
	d.data = nil
}

func (c *Cluster) rebalance() {
	// brute way is to just literally go thru every key in the system
	// and see if the key's GetNode pos doesn't match up
	c.accumulator.Init(c.getAllNodeAddrs())

	for _, node := range c.nodes {
		pairsMap := node.Store.memtable.GetAllKVPairs()

		for key, record := range pairsMap {
			newAddr, ok := c.hashRing.GetNode(key)
			if !ok {
				log.Printf("no node found for key %s during rebalance, skipping", key)
				continue
			}

			if newAddr != node.Addr {
				c.accumulator.Append(node.Addr, newAddr, record)
				node.Store.RemoveFromMemtable(key)
			}
		}
	}

	for srcNode, v := range c.accumulator.data {
		for destNode, pairs := range v {
			if len(pairs) > 0 {
				if err := c.transferDataBetweenNodes(srcNode, destNode, pairs); err != nil {
					log.Printf("failed to transfer %d keys from %s to %s: %v",
						len(pairs), srcNode, destNode, err)
				}
			}
		}
	}
	c.accumulator.ClearAccumulator()
}

func (c *Cluster) transferDataBetweenNodes(srcNodeAddr string, destNodeAddr string, data []*Record) error {
	client, conn, err := StartGRPCClient(destNodeAddr)
	if err != nil {
		return fmt.Errorf("connect to destination node %s: %w", destNodeAddr, err)
	}

	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Printf("close gRPC connection to %s: %v", destNodeAddr, err)
		}
	}(conn)

	kvPairs := convertRecordsToProtoKVPairs(data)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := client.MigrateKeyValuePairs(ctx, &proto.KeyValueMigrationRequest{
		SourceNodeAddr: srcNodeAddr,
		DestNodeAddr:   destNodeAddr,
		KvPairs:        kvPairs,
	})
	if err != nil {
		return fmt.Errorf("migrate key-value pairs to %s: %w", destNodeAddr, err)
	}

	if !res.Success {
		return fmt.Errorf("migration to %s reported failure", destNodeAddr)
	}

	return nil
}

func (c *Cluster) getAllNodeAddrs() []string {
	addrs := make([]string, 0, len(c.nodes))
	for addr := range c.nodes {
		addrs = append(addrs, addr)
	}
	return addrs
}

func convertProtoRecordToStoreRecord(record *proto.Record) *Record {
	return &Record{
		Header: Header{
			CheckSum:  record.Header.Checksum,
			Tombstone: uint8(record.Header.Tombstone),
			TimeStamp: record.Header.Timestamp,
			KeySize:   record.Header.KeySize,
			ValueSize: record.Header.ValueSize,
		},
		Key:        record.Key,
		Value:      record.Value,
		RecordSize: record.RecordSize,
	}
}

func convertRecordsToProtoKVPairs(records []*Record) []*proto.KVPair {
	kvPairs := make([]*proto.KVPair, 0, len(records))
	for _, rec := range records {
		kvPairs = append(kvPairs, &proto.KVPair{
			Record: &proto.Record{
				Header: &proto.Header{
					Checksum:  rec.Header.CheckSum,
					Tombstone: uint32(rec.Header.Tombstone),
					Timestamp: rec.Header.TimeStamp,
					KeySize:   rec.Header.KeySize,
					ValueSize: rec.Header.ValueSize,
				},
				Key:        rec.Key,
				Value:      rec.Value,
				RecordSize: rec.RecordSize,
			},
		})
	}
	return kvPairs
}
