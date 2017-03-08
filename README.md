# DXRAM: A distributed in-memory key-value storage

Developed by the [operating systems group](http://www.cs.hhu.de/en/research-groups/operating-systems.html) of the department of computer science of the Heinrich-Heine-University Düsseldorf, DXRAM is a distributed in-memory key-value store for low-latency cloud applications, e.g. social networks, search engines and I/O-bound long running scientific computations running in a data center.

Facebook for example runs around 1,000 memcached servers keeping around 75% of its data always in RAM because back-end database accesses are too slow. We argue that it is possible go one step further by keeping all data always in RAM thus relieving developers from cache management and synchronization of caches with secondary storage.

The storage service implements a crash-recovery failure-model providing scalable persistence. This is achieved by a smart asynchronous logging scheme storing replicated log items on remote nodes’ flash storage allowing fast recovery of huge amounts of data. Thus DXRAM can mask single and multiple node failures and even a full data center power outage.

The DXRAM core services implement a key/value data model aiming to support billions of small binary objects, e.g. for social network graphs. The distributed object lookup mechanism uses a super-peer overlay network in order to determine on which node a particular piece of data is located. The latter will also handle meta-data management, and should preserve elastic scalability and reliability of the system. Because RAM is expensive data is only replicated on flash disk for fault tolerance. Although in-memory data is not replicated in RAM, we support migrating hot spots dynamically to other machines.

The key challenge for a fast recovery of many small key-value tuples is a smart logging approach. Specific challenges arise from social applications that store billions of small objects requiring a low-overhead meta-data management for the log while preserving fast recovery of crashed nodes. This is only possible when logs are scattered over many backup nodes in order to avoid CPU, disk and network saturation.

## Quick start guide

Refer to [this readme](doc/QuickStart.md) on how to set up and run DXRAM instances using our deploy scripts.

## Using the DXRAM terminal

Refer to [this readme](doc/Terminal.md) on how to use the terminal.

## First applications and benchmarks

Refer to [this readme](doc/Benchmark.md) on how to run built in benchmarks or external applications/benchmarks.

## FAQ and troubleshooting

For FAQ refer to [this readme](doc/FAQ.md) and for troubleshooting of common errors refer to [this readme](doc/Troubleshooting.md).

## Architecture

An outline of the DXRAM architecture is given in [this readme](doc/Architecture.md).

## Publications

A list of publications that involve the DXRAM system or parts of its architecture is available [here](http://www.cs.hhu.de/en/research-groups/operating-systems/publications.html). These documents are a good source to get to know the system and its design decisions better and might help you when developing your own applications.

## Writing your own applications using DXRAM

Development setup can be found [here](doc/DevelopmentSetup.md) and information on how to develop applications using DXRAM [here](doc/Development.md).

## Manual configuration of DXRAM

Refer to [this readme](doc/ManualSetup.md) on how to set up DXRAM instances without the deploy script. This might be necessary if your environment has special requirements or does not support the deploy script properly.
