The DXRAM terminal is a peer node without any storage or backup capabilities. It joins the overlay like a regular peer and can access any other node of the system. The following steps assume you have started at least one superpeer and one peer before starting the terminal.

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

As stated by the greeting, you can use *?* or *help* to get some basic usage information about the terminal. The terminal provides built in commands for common tasks but also supports JavaScript. You can type and run JavaScript code in the terminal as well as load and run JavaScript files (further information below).

# First steps

To get a list of built in commands, type *list*. To get information and usage about a command use *help <cmd>* e.g. *help nodelist*.

Let's look around and see who else has joined the overlay by using the *nodelist* command:
```
Total available nodes (3):
	0xC0C1   superpeer
	0xC181   peer
	0x0140   terminal
```

As you can see, we are running a superpeer, a peer and the terminal we are currently on.

*nodeinfo* shows information about the current node:
```
Node info 0x140:
	Role: terminal
	Address: /127.0.0.1:22220
```

But you can also get information about another node with *nodeinfo(0xC181)*:
```
Node info 0xC181:
	Role: peer
	Address: /127.0.0.1:22222
```

# Accessing a storage node

You can use the terminal to create, get, put and remove chunks from any available storage node (peer). Let's start with creating a 64 byte chunk by entering *chunkcreate(64, 0xC181)*:
```
Created chunk of size 64: 0xC181000000000001
```

We can now check which chunks are available on our target peer by using *chunklist(0xC181)*:
```
Locally created chunk id ranges of 0xC181 (1):
[0xC181000000000000, 0xC181000000000001]
```

Furthermore, we can get detailed memory information about the key-value store using *chunkstatus(0xC181)*:
```
Free memory (gb): 0.12450653128325939
Total memory (gb): 0.125
Total payload memory (gb): 4.933299496769905E-4
Num active memory blocks: 7
Num active chunks: 2
Total chunk payload memory (gb): 1.1182762682437897E-4
Num CID tables: 5
Total CID tables memory (gb): 3.8150232285261154E-4
Num of free LIDs cached in LIDStore: 0
Num of total available free LIDs in LIDStore: 0
New LID counter state: 2
```

We haven't associated any data with the chunk we created, yet. Let's write the string "Hello key-value store!" to the chunk we just created with *chunkput(0xC181, 1, "Hello key-value store!")* or *chunkput("0xC181000000000001", "Hello key-value store!")*:
```
Put to chunk 0xC181000000000001 successful
```

Let's get the data and check if it was correctly written to the key-value store using *chunkget("0xC181000000000001")*:
```
Chunk data of 0xC181000000000001 (chunksize 64):
48 65 6c 6c 6f 20 6b 65 79 2d 76 61 6c 75 65 20 73 74 6f 72 65 21 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
```

In this case, a string representation of the data would be prefered. We can add further paramters for that to the command, e.g. *chunkget(0xC181, 1, "str")*:
```
Chunk data of 0xC181000000000001 (chunksize 64):
Hello key-value store!
```

Take a look at the usage information of the chunk commands. There are various possibilities to get/put with different data types as well.

Finally, we delete the chunk we created using *chunkremove("0xC181000000000001")*:
```
Chunk 0xC181000000000001 removed
```

If you try to access the chunk now, e.g. using *chunkget("0xC181000000000001")*, you get an error message:
```
Getting chunk 0xC181000000000001 failed
```

# Using the nameservice

You can register chunk IDs of existing chunks at the nameservice, e.g. *namereg("0xC181000000000001", "CHUNK")*. Using *namelist* you get a list of all registered entries at the nameservice:
```
Nameservice entries(1):
CHUNK: 0xC181000000000001
```

Resolving names is also possible using *nameget*, e.g. *nameget("CHUNK")*:
```
CHUNK: 0xC181000000000001
```

# Temporary storage on superpeer

The overlay provides a temporary storage which works similar to the key-value store on peers but is limited to a few kb/mb and the number of chunks that can be stored. It's supposed to be used for storing (intermediate) results of benchmarks or calculations and not for application data.

Using *tmpcreate*, *tmpget*, *tmpput* and *tmpremove* is very similar to the chunk calls before. Instead of a full 64-bit chunk ID, the key is limited to a 32-bit integer. Furthermore, it does not provide ID management. The user has to assign IDs to the stored data manually and take care of management.

Example:
```
$0x0140> tmpcreate(0, 64)
Created chunk of size 64 in temporary storage: 0x0
$0x0140> tmpput(0, "12345")
Put to chunk 0x0 to tmp storage successful
$0x0140> tmpget(0, "str")
Temp chunk data of 0x0 (chunksize 64):
12345
$0x0140> tmpstatus
Total size occupied (bytes): 64
Id: Size in bytes
0x0: 64
tmpremove(0)
Removed chunk with id 0x0 from temporary storage
```
