Benchmarks for ISMM17 paper

Requires a storage node with 64 GB of RAM and the MasterSlaveComputeService.
Run this using the deploy script and the configuration: ChunkBenchISMM17.conf

* RawMemory: Compare Unsafe and JNINativeMemory implementation 
* CIDTable cache: Compare CIDTable cache enabled and disabled
* Small objects scalability: 16 byte objects with batch counts 1-10 and thread count 1-16
* Scalability on other object sizes (24 bytes - 1 mb)
