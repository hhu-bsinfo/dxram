# DXRAM: A distributed in-memory key-value storage

<img src="doc/img/logo.jpg" width="500">

[![Build Status](https://travis-ci.org/hhu-bsinfo/dxram.svg?branch=master)](https://travis-ci.org/hhu-bsinfo/dxram)
[![Build Status](https://travis-ci.org/hhu-bsinfo/dxram.svg?branch=development)](https://travis-ci.org/hhu-bsinfo/dxram)

Developed by the [operating systems group](http://www.cs.hhu.de/en/research-groups/operating-systems.html)
of the department of computer science of the Heinrich-Heine-University 
Düsseldorf, DXRAM is a distributed in-memory key-value store for 
low-latency cloud applications, e.g. social networks, search engines 
and I/O-bound long running scientific computations running in a data 
center. It is designed to efficiently manage billions of small data 
objects which are typical for graph based applications.

For example, Facebook runs approx. 1,000 memcached servers keeping 
around 75% of its data always in RAM because back-end database accesses 
are too slow. We argue that it is possible to go one step further by 
keeping all data always in RAM relieving developers from cache 
management and synchronization of caches with secondary storage.

# Important

DXRAM is a research project that's under development. We do not 
recommend using the system in a production environment or with 
production data without having an external backup. Expect to encounter 
bugs. However, we are looking forward to bug reports and code 
contributions.

# Features

* Distributed object lookup and scalable meta-data management with 
super-peer overlay
* Optimized for up to 10 GBit/s Ethernet network and up to 56 GBit/s
InfiniBand
* All objects always in RAM
* Custom, highly efficient and performant memory management for 
billions of small objects (< 64 bytes) per node
* Smart asynchronous logging to disk (optimized for SSD) for persistency
* Replication of logs to remote nodes for fault tolerance
* Crash-recovery failure-model
* Object migrations for storage nodes to handle hot spots
* Run computations on storage nodes using lightweight jobs or 
tasks submitted to coordinated compute groups

# Contents

* [Quick start guide](doc/QuickStart.md): 
Setup and run DXRAM instances using our deploy scripts
* [Using the DXRAM terminal](doc/Terminal.md): 
Your first steps to get familiar with the environment
* [First applications and benchmarks](doc/Benchmark.md): 
Run built in benchmarks or external applications/benchmarks
* [FAQ](doc/FAQ.md)
* [Troubleshooting](doc/Troubleshooting.md)
* [DXRAM Architecture](doc/Architecture.md)
* [Publications involving DXRAM](http://www.cs.hhu.de/en/research-groups/operating-systems/publications.html)
* [Development setup](doc/DevelopmentSetup.md)
* [Develop applications using DXRAM](doc/Development.md)
* [Manual setup and configuration of DXRAM instances](doc/ManualSetup.md)

# License

Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf,
Institute of Computer Science, Department Operating Systems. 
Licensed under the [GNU General Public License](LICENSE.md).
