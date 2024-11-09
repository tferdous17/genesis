# genesis

**Table of Contents**
1. [About](#About)
2. [Architecture](#Architecture)
   1. [Memtable](##Memtable-(Red-Black Tree))
   2. [SSTable](##SSTable)
      1. [Compaction](###Compaction)
   4. [Write-Ahead-Log](##Write-Ahead-Log)
3. [Complete Tree](#Complete-Tree)
4. [Benchmarks](#Benchmarks)
   1. [Full Tree ](###Full-Tree)
5. [Feature Checklist](#Feature-Checklist)
6. [References](#References)

# About
genesis is a disk-based, log-structured merge (LSM) tree key-value store. This project was originally based off the Bitcask research paper (which doesn't use LSM), but expanded upon and redesigned.
Built purely for educational purposes.

# Architecture
Overview of the architecture, from the Memtable implemention, to how the data is stored on disk in the form of SSTables.

![LSM Architecture](extra/lsm.png)
Source: “A Survey of LSM-Tree Based Indexes, Data Systems and KV-Stores.” arxiv.org/html/2402.10460v2.

## Memtable (Red-Black Tree)
A [red black tree](https://en.wikipedia.org/wiki/Red%E2%80%93black_tree) is a self-balancing binary search tree (BST) with a couple distinct properties to ensure the tree remains balanced and maintains efficient search, insertion, and deletion operations in O(log(n)) time.

In this LSM tree implementation, a red-black tree is used as the memtable—which is the **in-memory** portion of this database in which all incoming writes are temporarily stored before being flushed to disk. Flushing to disk involves taking every record in the memtable and storing it in a Sorted String Table (SSTable) on disk, which is a process triggered under a defined threshold. This batching approach allows for much greater efficiency, as it minimizes costly disk writes.

As there's a natural, sorted ordering to elements in a BST, it makes a lot of sense to use a red-black tree when creating *Sorted* String Tables coupled with the performance gains of a self-balancing BST.

## SSTable
An SSTable (Sorted String Table) is a file format used for storing key-value pairs in a sorted order. It is commonly used in systems like LevelDB and Bigtable for efficient data storage in key-value stores.

**Components:**
- Data: all the key-value pairs in sorted order (by key) in a .data file
- Sparse index: index structure that stores a subset of the original keys and their corresponding byte offsets in the data file
- Bloom filter: a _probabilistic_ data structure used to test the membership of a given key in the SSTable

SSTables are **persisted** to disk immediately when created, and each table is represented by three files:
- <sst_num>.data
- <sst_num>.index
- <sst_num>.bloom

To **lookup a key**, the system will automatically look in the memtable first to check if they key is still in-memory and hasn't been flushed yet. If the key is not present in the memtable, then we start looking at the SSTables on disk. 
The general process to find a key on disk is the following:
- Use the bloom filter to check if a key _may or may not_ be in a given SSTable
- If the key is present, utilize binary search + the sparse index structure to find the maximum lower bound of the target key
- Scan every key-value pair starting from that offset until either 1) the key is found or 2) the scan overextends
- Repeat process until the target key is found

### Compaction
To improve overall performance and efficiency, genesis implements a [size-tiered compaction strategy](https://cassandra.apache.org/doc/stable/cassandra/operating/compaction/stcs.html) based off Apache Cassandra. This process merges multiple tables found within a bucket into 1 bigger, most-recent table. Essentially, it removes all outdated entries, performs garbage collection, and frees up disk space.

Compaction is automatically triggered when the memtable reaches a defined byte threshold.

## Write-Ahead-Log
genesis supports write-ahead-logging (WAL) to improve durability and serve as a **crash recovery** mechanism in the face of network faults. Upon each operation (put, get, delete), metadata (such as the operation) and other info such as the key/value is appended to an auto-generated .log file which can be used to reconstruct the state of the tree in the case of a crash. 

# Complete Tree
The complete tree is the seamless combination of the Memtable and SSTable component. When combined, we effectively have an in-memory component and a disk component. LSM trees were designed to emphasize **write performance**, which is also seen with genesis.

genesis supports the following operations:
- Put(key, value)
- Get(key)
- Delete(key)

Important to note is that genesis utilizes **tombstone-based garbage collection**. When deleting an existing key, it will simply append a tombstone value in the header and re-add it to the memtable (which will eventually get flushed to disk). The _actual_ deletion process occurs in the SSTable compaction algorithm.


# Benchmarks (11/8/24)
### Full Tree
- Put: insert 1,000,000 distinct kv pairs to the tree
```go
goos: darwin
goarch: arm64
cpu: Apple M3 Pro
                                 Ops                Time per op         Ops per sec
BenchmarkDiskStore_Put-12    	 1000000	     9745 ns/op	         104326 ops/s
BenchmarkDiskStore_Put-12    	 1000000	     9743 ns/op	         104354 ops/s
BenchmarkDiskStore_Put-12    	 1000000	     9740 ns/op	         104464 ops/s
BenchmarkDiskStore_Put-12    	 1000000	     9745 ns/op	         104327 ops/s
BenchmarkDiskStore_Put-12    	 1000000	     9752 ns/op	         104146 ops/s
```


# Feature Checklist
Features in the works:
- [x] Serialize/Deserialize header + key, value
- [x] Store data on disk
- [x] Support Put(Key, Value)
- [x] Support Get(Key)
- [x] Support Delete(Key)
- [x] Crash safety (CRC)
- [x] Convert implementation to Log-Structured Merge Tree
  - [x] Swap out keydir with red-black tree memtable
    - [x] Implement red-black tree
  - [x] Write-ahead-logging (WAL)
    - [x] Create WAL file and write to it after Put(k, v) operations 
    - [ ] Reconstruct memtable with WAL in case of crash
  - [x] Implement SSTables
    - [x] Flush memtable to data file in sorted order
      - [x] Conditional flushing (size threshold)
    - [x] Index file
    - [x] Bloom filter
    - [x] Multiple levels (higher levels store larger, compacted tables)
    - [x] Get(key) operation on tables
    - [x] Size-Tiered Compaction (based off Apache Cassandra)
- [ ] Make this distributed
  - [ ] Data partitioning (sharding)
    - [ ] Choose hashing strategy
    - [ ] Create the actual nodes/shards
  - [ ] Replication for fault tolerance
  - [ ] Consensus algorithm (Paxos/Raft)
  - [ ] Handle node failure
  - [ ] Inter-node communication (RPC?)
- [ ] (EXTRA) Generic key/value support (currently limited to strings)

# References
- LSM Tree (ScyllaDB) - https://www.scylladb.com/glossary/log-structured-merge-tree/
- LSM Tree - https://itnext.io/log-structured-merge-tree-a79241c959e3
- LSM Trees: the Go-To Data Structure - https://medium.com/@dwivedi.ankit21/lsm-trees-the-go-to-data-structure-for-databases-search-engines-and-more-c3a48fa469d2
- Deep Dive Into LSM Trees - https://medium.com/@lokeshbihani99/part-2-deep-dive-into-lsm-trees-structure-optimization-and-benefits-ef962410abf6
- LSM Tree + SSTables (Video) - https://youtu.be/ciGAVER_erw?si=2VJ_5KvjG6Szujke
- A Survey of LSM-Tree based Indexes, Data Systems and KV-stores - https://arxiv.org/html/2402.10460v2
- Bloom Filters - https://harish-bhattbhatt.medium.com/bloom-filter-application-and-implementation-52c6d4512c21
- Size Tiered Compaction - https://cassandra.apache.org/doc/stable/cassandra/operating/compaction/stcs.html
- Sized Tiered Compaction more -https://shrikantbang.wordpress.com/2014/04/22/size-tiered-compaction-strategy-in-apache-cassandra/
