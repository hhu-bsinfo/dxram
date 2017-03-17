# ChunkServiceLocalBench

Single storage node benchmark to test the maximum local throughput of a storage node (DXRAM peer). The script requires at least one slave node as part of a compute group with at least 10 GB memory for the key value store.

Use the *script/deploy/conf/bench/ChunkServiceBench.conf* config as a template and adjust the parameters (e.g. kvss, target nodes if different to localhost).

# ChunkServiceRemoteBench
Benchmark requiring at least two peers to test the remote throughput of storage nodes. The script requires at least two slave nodes as part of a compute group with at least 10 GB memory for the key value store.

Use the *script/deploy/conf/bench/ChunkServiceBench.conf* config as a template and adjust the parameters (e.g. kvss, target nodes if different to localhost). You can add more (storage) peers as well if you want to run the benchmark with more instances involved.

This benchmark puts a high load on the network subsystem. Thus, it might happen that the system outputs error messages about dropped requests, e.g.
```
Sending chunk get request to peer 0x140 failed: de.hhu.bsinfo.ethnet.NetworkResponseTimeoutException: Waiting for response from node 0x0140 failed, timeout
```

In this case, the error can be ignored if the target node is still up and running. Due to high load the target node wasn't able to handle the high message load for a brief moment which isn't crucial for this benchmark.

You can try increasing the message handler threads for the DXRAM nodes in the DXRAM configuration:
```
"NetworkComponent": {
  "m_threadCountMsgHandler": 1,
  ...
```

That value sets the number of threads to spawn for handling and processing incoming messages. Be aware that increasing the thread count here might reduce the overall performance if your CPU load is already at its limits.
