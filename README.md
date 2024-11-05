# genesis

genesis is a disk-based, log-structured ~~hash table~~ merge tree KV store originally built upon the Bitcask research paper.
Built purely for educational purposes.

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
- [ ] (EXTRA) Generic key/value support (currently limited to strings)
