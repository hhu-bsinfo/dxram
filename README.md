# Architecture

An outline of the DXRAM architecture is given in [this document](doc/Architecture.md).

# Publications

All publications with DXRAM are listed in [this document](doc/Publications.md).

# Development

Development setup can be found [here](doc/DevelopmentSetup.md) and information on how to develop applications using DXRAM [here](doc/Development.md).

# Quick start guide

## Requirements
DXRAM requires Java 1.8 to run on a Linux distribution of your choice (MacOSX should work as well but is not supported, officially). DXRAM uses zookeeper (TODO link to zookeeper website) for bootstrapping.

### Dependencies
The following dependencies are required and are already included with DXRAM in the "lib" folder:
* ant-contrib (0.6) for building
* gson (2.7) for DXRAM
* jline (1.0) for DXRAM
* Log4j (version 1) for Zookeeper
* Log4j2 (2.7) for DXRAM
* sl4j (version 1) for Zookeeper
* sl4j-log4j12 for Zookeeper
* zokeeper (3.4.3) for DXRAM

## ZooKeeper
Download, install and setup ZooKeeper (TODO link to zookeeper page). See System Requirements in the admin guide of ZooKeeper.

## Building
An ant build script is included for building the project. Ensure ant is installed and run *build.sh*. The build output is located in the *build/* subdirectory.

## Deploying to a remote machine (cluster)
Copy the *build/dxram* directory to your cluster.

## Starting DXRAM
A bash script to easily deploy instances to either local host or nodes of a cluster is included (*script/deploy/deploy.sh*). The script parses a configuration file and starts the specified DXRAM instances. For examples, refer to the configurations in the subfolders *script/deploy/conf*, especially the *examples* category with *SimpleTest.conf*. For further details on deployment refer to [this readme](script/deploy/README.md).

To run a minimal DXRAM setup, compile dxram using the *build.sh* script. Adjust the paths at the top of the *SimpleTest.conf* and run the deploy script with the *SimpleTest.conf* from the root of the dxram folder:
```
./build.sh
./script/deploy/deploy.sh ./script/deploy/conf/example/SimpleTest.conf
```
The example starts zookeeper, one superpeer, one peer and the terminal, locally. Any log output of the instances is written to log files in a directory called *xxx_deploy_tmp/logs*. If you encounter any errors, check out the log files of each instance first.

# First applications and benchmarks

Refer to [this readme](doc/Benchmark.md) on how to run built in benchmarks or external applications/benchmarks.

# Writing your own applications using DXRAM

Refer to [this readme](doc/Development.md) on how to develop your own applications with DXRAM.

# Manual configuration of DXRAM

The next sections provide detailed information on how to set up DXRAM without the included deploy scripts. This might be necessary if your environment has special requirements or does not support the deploy script properly. 

## Log4j2 configuration
DXRAM uses Log4j2 for logging. A configuration file for Log4j2 can be provided by is not required. The logger will default to logging to console without a configuration file. To run DXRAM with a configuration use the vm argument:
```
-Dlog4j.configurationFile=config/log4j.xml
```

An example for a Log4j2 configuration file printing to the console (async mode) with coloring enabled is provided in the *config/* subdirectory.

## DXRAM configuration 
DXRAM is configured using a JSON formatted configuration file. A default configuration file is created on the first start of either a Superpeer or Peer if the configuration file does not exist. Make sure the path/folders where the configuration should be located exists (i.e. *config/* subfolder for the default path). 

### DXRAM vm arguments
You can start DXRAM with a single configuration parameter provided via vm arguments:
```
-Ddxram.config=config/dxram.json
```

If the configuration does not exists, a default configuration is created in that place. Further parameters denote variables of the internal class _DXRAMContext_ and are applied either from the configuration file or
overriden using vm arguments. For example, to run a superpeer, the following set of arguments 
overrides the equivalent parameters from the configuration and runs a Superpeer locally on port 22221:
```
-Ddxram.m_engineSettings.m_address.m_ip=127.0.0.1
-Ddxram.m_engineSettings.m_address.m_port=22221
-Ddxram.m_engineSettings.m_role=Superpeer
```
And equivalent for a Peer on port 22222:
```
-Ddxram.m_engineSettings.m_address.m_ip=127.0.0.1
-Ddxram.m_engineSettings.m_address.m_port=22222
-Ddxram.m_engineSettings.m_role=Peer
```

You could create one configuration file for each DXRAM instance of course. But, most parameters are shared on all instances and overriding let's you change just a few selected parameters easily without having multiple configurations.

### Nodes configuration
Depending on your setup (local or cluster) you have to adjust the configuration of the nodes. Every node needs an IP and a free port assigned. The nodes configuration needs to be the same for all instances. The roles assignable are superpeer (S) and peer (P). Terminals are not listed in the nodes configuration. The nodes list is part of DXRAM's configuration file and, in the source code, located in the _ZookeeperBootComponent_ class. 
The default configuration defines one local Superpeer (port 22221) and two local Peers (ports 22222 and 
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

Items like "m_rack", "m_switch" and "m_readFromFile" can be left out if not required and will be set to default values, automatically.

Furthermore, the IP and port of the node running ZooKeeper needs to be specified as well to allow boostrapping.

```json
    "ZookeeperBootComponent": {
      "m_path": "/dxram",
      "m_connection": {
        "m_ip": "127.0.0.1",
        "m_port": 2181
      },
```

#### Storage size
To increase/decrease the ram size for storing data in DXRAM, modify the following entry under  "MemoryManagerComponent":
```json
"m_keyValueStoreSize": {
    "m_value": 134217728,
    "m_unit": "b"
}
```
Different units include "b", "kb", "mb", "gb" and "tb". Or add a vm argument when starting a DXRAM instance to override the value provided by the configuration, for example (128 MB):
```
-Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_value=128
-Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_unit=mb
```
