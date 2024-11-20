package store

import (
	"bitcask-go/http"
	"bitcask-go/proto"
	"bitcask-go/utils"
	"context"
	"fmt"
	"github.com/serialx/hashring"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

type Node struct {
	ID    string
	Addr  string
	Store *DiskStore
}

type Cluster struct {
	hashRing *hashring.HashRing
	nodes    map[string]*Node
}

var nodeCounter uint32 = 1
var currentNodePort uint32 = 11000

func (c *Cluster) initNodes(numOfNodes int) {
	c.nodes = make(map[string]*Node)
	var nodeAddrs []string

	for i := 0; i < numOfNodes; i++ {
		store, _ := NewDiskStore()
		node := Node{
			ID:    fmt.Sprintf("node-%d", nodeCounter),
			Addr:  fmt.Sprintf(":%d", currentNodePort),
			Store: store,
		}
		c.nodes[node.Addr] = &node

		StartGRPCServer(node.Addr, &node)

		atomic.AddUint32(&currentNodePort, 1)
		atomic.AddUint32(&nodeCounter, 1)
		nodeAddrs = append(nodeAddrs, node.Addr)
	}

	c.hashRing = hashring.New(nodeAddrs)
}

func (c *Cluster) TransferDataBetweenNodes(srcNodeAddr string, destNodeServerAddr string) {
	client, conn := StartGRPCClient(destNodeServerAddr)
	defer conn.Close()

	rec := []Record{
		{
			Header: Header{
				CheckSum:  1,
				Tombstone: 2,
				TimeStamp: 3,
				KeySize:   4,
				ValueSize: 5,
			},
			Key:        "tesdfdsfds",
			Value:      "dbvdbdf",
			RecordSize: 10,
		},
		{
			Header: Header{
				CheckSum:  4,
				Tombstone: 1,
				TimeStamp: 7,
				KeySize:   6,
				ValueSize: 5,
			},
			Key:        "dfgdfgdfg",
			Value:      "xvv",
			RecordSize: 14,
		},
	}

	kvPairs := convertRecordsToProtoKVPairs(&rec)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := client.MigrateKeyValuePairs(ctx, &proto.KeyValueMigrationRequest{
		SourceNodeAddr: srcNodeAddr,
		DestNodeAddr:   destNodeServerAddr,
		KvPairs:        kvPairs,
	})
	if err != nil {
		utils.LogRED("err = %s", err)
	}

	fmt.Println(res)
	//transferKVPairs(client, srcNodeAddr, destNodeServerAddr, &rec)
}

func (c *Cluster) AddNode() {
	store, _ := NewDiskStore()
	node := Node{
		ID:    fmt.Sprintf("node-%d", nodeCounter),
		Addr:  fmt.Sprintf(":%d", currentNodePort),
		Store: store,
	}
	c.nodes[node.Addr] = &node
	atomic.AddUint32(&nodeCounter, 1)
	atomic.AddUint32(&currentNodePort, 1)

	// ? reassign hashring to whats returned by AddNode()?
	c.hashRing = c.hashRing.AddNode(node.Addr)
}

func (c *Cluster) RemoveNode(addr string) {
	_, ok := c.nodes[addr]
	if ok {
		c.hashRing.RemoveNode(addr)
	} else {
		fmt.Printf("node @ addr %s not found", addr)
	}
}

var defaultPort = ":8080"

func (c *Cluster) Open() {
	clusterService := http.NewClusterService(defaultPort, c)
	clusterService.Start()

	fmt.Println("HTTP server started successfully")
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	fmt.Println("calling transfer nodes")
	// * works
	c.TransferDataBetweenNodes(":11000", ":11001")

	res, err := c.nodes[":11001"].Store.Get("dfgdfgdfg")
	if err != nil {
		utils.LogRED("err = %s", err)
	}
	utils.LogYELLOW("res after transferring = %s", res)

	// Block until one of the signals above is received
	select {
	case <-signalCh:
		c.PrintDiagnostics()
		log.Println("signal received, shutting down...")
		err := clusterService.Close()
		if err != nil {
			fmt.Println(err)
		}

	}
}

func (c *Cluster) Put(key, value string) error {
	nodeAddr, _ := c.hashRing.GetNode(key) // get which node this key should be on
	fmt.Printf("key = %s\t", key)
	fmt.Printf("added @ node addr = %s\n", nodeAddr)

	node, ok := c.nodes[nodeAddr]

	if ok {
		return node.Store.Put(&key, &value)
	}
	return nil
}

func (c *Cluster) Get(key string) (string, error) {
	fmt.Printf("key = %s\t", key)
	nodeAddr, _ := c.hashRing.GetNode(key) // get which node this key should be on
	node, ok := c.nodes[nodeAddr]

	if ok {
		fmt.Printf("found @ node addr = %s\n", nodeAddr)
		return node.Store.Get(key)
	}

	return "", nil
}

func (c *Cluster) Delete(key string) error {
	nodeAddr, _ := c.hashRing.GetNode(key) // get which node this key should be on
	node, ok := c.nodes[nodeAddr]

	if ok {
		fmt.Printf("deleted @ node addr = %s\n", nodeAddr)
		return node.Store.Delete(key)
	}

	return nil
}

func (c *Cluster) PrintDiagnostics() {
	fmt.Println("DIAGNOSTICS:")
	for _, v := range c.nodes {
		fmt.Printf(v.ID + " @ address " + v.Addr + " , num keys: ")
		v.Store.LengthOfMemtable()
	}
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

func convertRecordsToProtoKVPairs(records *[]Record) []*proto.KVPair {
	var KVPairs []*proto.KVPair
	for _, rec := range *records {
		convRec := &proto.KVPair{
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
		}

		KVPairs = append(KVPairs, convRec)
	}

	return KVPairs
}
