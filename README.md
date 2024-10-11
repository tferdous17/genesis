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
- [ ] Convert implementation to Log-Structured Merge Tree
  - [x] Swap out keydir with red-black tree memtable
    - [x] Implement red-black tree
  - [x] Write-ahead-logging (WAL)
    - [x] Create WAL file and write to it after Put(k, v) operations 
    - [ ] Reconstruct memtable with WAL in case of crash
  - [ ] Implement SSTables
    - [x] Flush memtable to data file in sorted order
      - [ ] Periodic and/or conditional flushing
    - [x] Index file
    - [ ] Bloom filter
    - [ ] Multiple levels
    - [x] Get(key) operation on tables
    - [ ] Compaction
- [ ] Generic key/value support (currently limited to strings)
- [ ] Make this distributed
