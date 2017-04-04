The DXRAM terminal is a peer node without any storage or backup capabilities. It joins the overlay like a regular peer and can access any other node of the system and can be used to experiment with the setup. Debugging capabilities are limited but it can be useful to look at single data objects on a live deployment/experiment if something went wrong. Furthermore, it can be extended with custom terminal commands. The following steps assume you have started at least one superpeer and one peer before starting the terminal.

# Launching the terminal

To launch the terminal after all other instances are deployed, add it as a last instance to your deploy configuration, e.g. for localhost:
```
localhost,T
```
The terminal will automatically spawn. If everything's good, it greets you with some output:
```
>>> DXRAM Node <<<
Cwd: /home/user/dxram
NodeID: 0x0140
Role: terminal
Address: /127.0.0.1:22220
!---ooo---!
>>> DXRAM Terminal started <<<
Running on node 0x0140, role terminal
Type '?' or 'help' to print the help message
$0x0140>
```
Note that the *NodeID* and other parameters may differ on your machine.

Launching the terminal independent of the deploy script is possible but currently we don't have scripts for that.

As stated by the greeting, you can use *?* or *help* to get some basic usage information about the terminal. The terminal provides built in commands for common tasks.

# First steps

To get a list of built in commands, type *list*. To get information and usage about a command use *help <cmd>* e.g. *help nodelist*.

Let's look around and see who else has joined the overlay:
```
$0x0140> nodelist
Total available nodes (3):
	0xC0C1   superpeer
	0xC181   peer
	0x0140   terminal
```

As you can see, we are running a superpeer (node ID 0xC0C1), a peer (node ID 0xC181) and the terminal (node ID 0x0140).
Let's show some more information about the current node:
```
$0x0140> nodeinfo
Node info 0x140:
	Role: terminal
	Address: /127.0.0.1:22220
```

You can also get information about another node:
```
$0x0140> nodeinfo c181
Node info 0xC181:
	Role: peer
	Address: /127.0.0.1:22222
```

# Accessing a storage node

You can use the terminal to create, get, put and remove chunks from any available storage node (peer). Let's start with creating a 64 byte chunk on the peer 0xC181:
```
$0x0140> chunkcreate 64 c181
Created chunk of size 64: 0xC181000000000001
```

You can check which chunks are available on our target peer:
```
$0x0140> chunklist c181
Locally created chunk id ranges of 0xC181 (1):
[0xC181000000000000, 0xC181000000000001]
```

Furthermore, you can get detailed memory information about the key-value store:
```
$0x0140> chunkstatus c181
Free memory: 127 mb
Total memory: 128 mb
Total payload memory: 517 kb
Num active memory blocks: 7
Num active chunks: 2
Total chunk payload memory: 117 kb
Num CID tables: 5
Total CID tables memory: 400 kb
Num of free LIDs cached in LIDStore: 0
Num of total available free LIDs in LIDStore: 0
New LID counter state: 2

```

A lot of detailed information about the memory management is displayed to help debugging. "Free memory" and "Total memory" are most likely the only items that are relevant for now. If you are interested in details about our memory management, checkout our publications.

You haven't associated any data with the chunk created, yet. Let's write the string "Hello key-value store!" to the chunk:
```
$0x0140> chunkput c181 1 "Hello key-value store!"
Put to chunk 0xC181000000000001 successful
```

Also possible:
```
$0x0140> chunkput c181000000000001 "Hello key-value store!"
```

Let's get the data and check if it was correctly written to the key-value store:
```
$0x0140> chunkget c181000000000001
Chunk data of 0xC181000000000001 (chunksize 64):
48 65 6c 6c 6f 20 6b 65 79 2d 76 61 6c 75 65 20 73 74 6f 72 65 21 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
```

In this case, a string representation of the data is preferred. You can add further parameters for that to the command:
```
$0x0140> chunkget c181 1 str
Chunk data of 0xC181000000000001 (chunksize 64):
Hello key-value store!
```

Take a look at the usage information of the chunk commands. There are various possibilities to get/put with different data types as well.

Finally, delete the chunk:
```
$0x0140> chunkremove c181000000000001
Chunk 0xC181000000000001 removed
```

If you try to access the chunk now, you get an error message:
```
$0x0140> chunkget c181000000000001
Getting chunk 0xC181000000000001 failed
```

# Using the nameservice

You can register chunk IDs of existing chunks at the nameservice.
```
$0x0140> namereg c181000000000001 CHUNK
```

Get a list of all registered entries at the nameservice:
```
$0x0140> namelist
Nameservice entries(1):
CHUNK: 0xC181000000000001
```

Resolving names is also possible:
```
$0x0140> nameget CHUNK
CHUNK: 0xC181000000000001
```

# Temporary storage on superpeer

The overlay provides a temporary storage which works similar to the key-value store on peers but is limited to a few kb/mb and the number of chunks that can be stored. It's supposed to be used for storing (intermediate) results of benchmarks or calculations and not for application data.

Using *tmpcreate*, *tmpget*, *tmpput* and *tmpremove* is very similar to the chunk calls before. Instead of a full 64-bit chunk ID, the key is limited to a 32-bit integer. Furthermore, it does not provide ID management. The user has to assign IDs to the stored data manually and take care of management.

Example:
```
$0x0140> tmpcreate 0 64
Created chunk of size 64 in temporary storage: 0x0
$0x0140> tmpput 0 12345
Put to chunk 0x0 to tmp storage successful
$0x0140> tmpget 0 str
Chunk data of 0x0 (chunksize 68):
12345
$0x0140> tmpstatus
Total size occupied (bytes): 64
Id: Size in bytes
0x0: 64
$0x0140> tmpremove 0
Removed chunk with id 0x0 from temporary storage
```
