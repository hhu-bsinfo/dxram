# DXRAM: A distributed in-memory key-value storage

Developed by the [operating systems group](http://www.cs.hhu.de/en/research-groups/operating-systems.html) of the department of computer science of the Heinrich-Heine-University Düsseldorf, DXRAM is a distributed in-memory key-value store for low-latency cloud applications, e.g. social networks, search engines and I/O-bound long running scientific computations running in a data center.

Facebook for example runs around 1,000 memcached servers keeping around 75% of its data always in RAM because back-end database accesses are too slow. We argue that it is possible go one step further by keeping all data always in RAM thus relieving developers from cache management and synchronization of caches with secondary storage.

# Features

* Distributed object lookup and sclable meta-data management with super-peer overlay
* All objects always in RAM
* Custom, highly efficient and performant memory management for billions of small objects (< 64 bytes) per node
* Smart asynchronous logging to disk (optimized for SSD) for persistency
* Replication of logs to remote nodes for fault tolerance
* Crash-recovery failure-model
* Object migrations for storage nodes to handle hot spots
* Run computations on storage nodes using lightweight jobs or tasks submitted to coordinated compute groups

# Contents

* [Quick start guide](doc/QuickStart.md): Setup and run DXRAM instances using our deploy scripts
* [Using the DXRAM terminal](doc/Terminal.md)
* [First applications and benchmarks](doc/Benchmark.md): Run built in benchmarks or external applications/benchmarks
* [FAQ](doc/FAQ.md)
* [Troubleshooting](doc/Troubleshooting.md)
* [DXRAM Architecture](doc/Architecture.md)
* [Publications involving DXRAM](http://www.cs.hhu.de/en/research-groups/operating-systems/publications.html)
* [Development environement setup](doc/DevelopmentSetup.md)
* [Develop applications using DXRAM](doc/Development.md)
* [Manual setup and configuration of DXRAM instances](doc/ManualSetup.md)

# License

Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems. Licensed under the [GNU General Public License](LICENSE.md).
