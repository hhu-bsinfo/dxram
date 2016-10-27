# DXRAM

## Requirements
DXRAM needs Java 1.8 to run properly. Currently supported operating 
systems are MacOSX and Linux. DXRAM uses zookeeper for bootstrapping.

### Dependencies
The following dependencies are required and are already included with 
DXRAM in the "lib" folder:
* Gson (>= 2.7) for DXRAM
* Log4j (version 1) for Zookeeper
* Log4j2 (>= 2.7) for DXRAM
* sl4j (version 1) for Zookeeper
* sl4j-log4j12 for Zookeeper
* zokeeper (>= 3.4.3) for DXRAM

## Project structure
Clone the project and set it up like this:
```
workspace_dxram/
->dxram/
->zookeeper/
->start_zookeeper.sh
```
You can unpack zookeeper from the subfolder "util". The script is located
in the subfolder "util", too.

### ZooKeeper
See System Requirements in the Admin guide of ZooKeeper.

## IDE configuration
We are using IntelliJ for development and provide setup guidance for it.
Any other IDE supporting Java (like Eclipse) should work as well but
requires you to take care of the setup on your own.

### Configuration with IntelliJ
Open IntelliJ and create a new project from existing sources and 
choose the dxram folder from your workspace. Add the libraries under
_Project Structure_ in the _Libraries_ tab. Create run configurations
for a Superpeer and a Peer using the DXRAMMain class as the application
entry point and the following VM arguments for the Superpeer:
```
-Dlog4j.configurationFile=config/log4j.xml
-Ddxram.config=config/dxram.json
-Ddxram.m_engineSettings.m_address.m_ip=127.0.0.1
-Ddxram.m_engineSettings.m_address.m_port=22221
-Ddxram.m_engineSettings.m_role=Superpeer
```
...and the Peer:
```
-Dlog4j.configurationFile=config/log4j.xml
-Ddxram.config=config/dxram.json
-Ddxram.m_engineSettings.m_address.m_ip=127.0.0.1
-Ddxram.m_engineSettings.m_address.m_port=22222
-Ddxram.m_engineSettings.m_role=Peer
-Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_value=128
-Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_unit=mb
```

## Configuration
### DXRAM configuration 
DXRAM is configured using a JSON formated configuration file.
A default configuration file is created on the first start of either
a Superpeer or Peer if the configuration file does not exist.
Make sure the path/folders exist (i.e. config subfolder for the 
default path). 

### Log4j2 configuration
Furthermore, a configuration file for Log4j2 can be provided by is not
required. The logger will default to logging to console, only, 
without a configuration file. A configuration is provided using
vm arguments, example:
```
-Dlog4j.configurationFile=config/log4j.xml
```

An example for a Log4j2 configuration file printing to the console 
(async mode) with coloring enabled:
```
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="warn">
    <Appenders>
		<Console name="Console" target="SYSTEM_OUT">
			<PatternLayout pattern="%highlight{[%d{HH:mm:ss.SSS}][%t][%-5level][%logger{36}] %msg%n}{FATAL=red, ERROR=red, WARN=yellow, INFO=blue, DEBUG=green, TRACE=white}"/>
		</Console>
		<Async name="ConsoleAsync" bufferSize="500">
			<AppenderRef ref="Console"/>
		</Async>
  	</Appenders>
  	<Loggers>
    	<Root level="debug">
      		<AppenderRef ref="ConsoleAsync"/>
    	</Root>
  	</Loggers>
</Configuration>
```

### DXRAM vm arguments
You can start DXRAM with a single configuration parameter provided
to the application via vm arguments:
```
-Ddxram.config=config/dxram.json
```

If the configuration does not exists, a default configuration is 
created. Further parameters denote variables of the internal class
_DXRAMContext_ and can be applied from the configuration file or
overriden using vm arguments.
To run a superpeer, the following set of arguments overrides the equivalent
values from the configuration and runs a Superpeer locally on port 22221:
```
-Ddxram.m_engineSettings.m_address.m_ip=127.0.0.1
-Ddxram.m_engineSettings.m_address.m_port=22221
-Ddxram.m_engineSettings.m_role=Superpeer
```
And the same for a Peer on port 22222:
```
-Ddxram.m_engineSettings.m_address.m_ip=127.0.0.1
-Ddxram.m_engineSettings.m_address.m_port=22222
-Ddxram.m_engineSettings.m_role=Peer
```

### Nodes configuration
Depending on your setup (local or cluster) you have to adjust the configuration
of the nodes. Every node needs an IP and a free port assigned. The nodes
configuration needs to be the same for all instances. The roles assignable
are superpeer (S) and peer (P). Terminals are not listed in the nodes configuration.
The nodes list is part of DXRAM's configuration file and located
under the _ZookeeperBootComponent_. The default configuration defines
one local Superpeer (port 22221) and two local Peers (ports 22222 and 
22223):
```json
"m_nodesConfig": [
    {
      "m_address": {
        "m_ip": "127.0.0.1",
        "m_port": 22221
      },
      "m_role": "SUPERPEER",
      "m_rack": 0,
      "m_switch": 0,
      "m_readFromFile": 1
    },
    {
      "m_address": {
        "m_ip": "127.0.0.1",
        "m_port": 22222
      },
      "m_role": "PEER",
      "m_rack": 0,
      "m_switch": 0,
      "m_readFromFile": 1
    },
    {
      "m_address": {
        "m_ip": "127.0.0.1",
        "m_port": 22223
      },
      "m_role": "PEER",
      "m_rack": 0,
      "m_switch": 0,
      "m_readFromFile": 1
    }
]
```

Items like "m_rack", "m_switch" and "m_readFromFile" can be left out
if not required and will be set to default values, automatically.

Furthermore, the IP and port of the node running zookeeper needs to be
specified as well to allow boostrapping.

```json
"m_connection": {
    "m_ip": "127.0.0.1",
    "m_port": 2181
}
```

#### Storage size
To increase/decrease the ram size for storing data in DXRAM, modify the
following entry under "MemoryManagerComponent":
```json
"m_keyValueStoreSize": {
    "m_value": 134217728,
    "m_unit": "b"
}
```
Different units include "b", "kb", "mb", "gb" and "tb".
Or add a vm argument to override the value provided by the configuration, 
for example (128 MB):
```
-Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_value=128
-Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_unit=mb
```

## Test and Run
Execute the _start_zookeeper.sh_ in a terminal to start ZooKeeper. Now 
run the project in IntelliJ via the run configurations or from the 
terminal with the appropriate arguments. First you have to 
run the Superpeer, then the Peer.

## Build DXRAM as deployable jar
The Ant build-script "dxram-build.xml" creates a folder "build" under the 
project directory. The folder contains the jar-file for DXRAM and three 
additional folders "jni" and "lib" needed to run the jar.

## Running the jar file
The jar-file can run the core DXRAM system. To run a superpeer locally use:
```bash
java \
-Ddxram.config=config/dxram.json \
-Ddxram.m_engineSettings.m_address.m_ip=127.0.0.1 \
-Ddxram.m_engineSettings.m_address.m_port=22221 \
-Ddxram.m_engineSettings.m_role=Superpeer \
-cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:bin/ \
de.hhu.bsinfo.dxram.run.DXRAMMain
```
To run the peer locally:
The jar-file can run the core DXRAM system. To run a superpeer locally use:
```bash
java \
-Ddxram.config=config/dxram.json \
-Ddxram.m_engineSettings.m_address.m_ip=127.0.0.1 \
-Ddxram.m_engineSettings.m_address.m_port=22222 \
-Ddxram.m_engineSettings.m_role=Peer \
-Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_value=128 \
-Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_unit=mb \
-cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:bin/ \
de.hhu.bsinfo.dxram.run.DXRAMMain
```

When running this on a distributed setup (like a cluster), make sure to adjust the network addresses.

## Style guides
TODO update
