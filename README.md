# casklite

casklite is a disk-based, log-structured hash table KV store based on the Bitcask research paper.
Built purely for educational purposes.

Features in the works:
- [x] Serialize/Deserialize header + key, value
- [x] Implement in-memory hashtable (keydir)
- [x] Store data on disk
- [x] Support Put(Key, Value)
- [x] Support Get(Key)