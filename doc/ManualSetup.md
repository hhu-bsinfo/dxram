# Log4j2 configuration
DXRAM uses Log4j2 for logging. A configuration file for Log4j2 can be provided but is not required. The logger will default to logging to console without a configuration file. To run DXRAM with a configuration use the VM argument:
```
-Dlog4j.configurationFile=config/log4j.xml
```

An example for a Log4j2 configuration file printing to the console (async mode) with coloring enabled is provided in the *config/* sub-directory.

# DXRAM configuration
DXRAM is configured using a JSON formatted configuration file. A default configuration file is created on the first start of either a superpeer or peer if the configuration file does not exist. Make sure the path/folders where the configuration should be located exists (i.e. *config/* sub-folder for the default path).

## DXRAM vm arguments
You can start DXRAM with a single configuration parameter provided via VM arguments:
```
-Ddxram.config=config/dxram.json
```

If the configuration does not exists, a default configuration is created in that place. Further parameters denote variables of the internal class _DXRAMContext_ and are applied either from the configuration file or
overridden using VM arguments. For example, to run a superpeer, the following set of arguments
overrides the equivalent parameters from the configuration and runs a superpeer locally on port 22221:
```
-Ddxram.m_config.m_engineConfig.m_address.m_ip=127.0.0.1
-Ddxram.m_config.m_engineConfig.m_address.m_port=22221
-Ddxram.m_config.m_engineConfig.m_role=Superpeer
```
And equivalent for a Peer on port 22222:
```
-Ddxram.m_config.m_engineConfig.m_address.m_ip=127.0.0.1
-Ddxram.m_config.m_engineConfig.m_address.m_port=22222
-Ddxram.m_config.m_engineConfig.m_role=Peer
```

You could create one configuration file for each DXRAM instance of course. But, most parameters are shared on all instances and overriding let's you change just a few selected parameters easily without having multiple configurations.

## Nodes configuration
Depending on your setup (local or cluster) you have to adjust the configuration of the nodes. Every node needs an IP and a free port assigned. The nodes configuration needs to be the same for all instances. The roles assignable are superpeer (S) and peer (P). The nodes list is part of DXRAM's configuration file and, in the source code, located in the _ZookeeperBootComponent_ class.
The default configuration defines one local superpeer (port 22221) and two local peers (ports 22222 and
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

Furthermore, the IP and port of the node running ZooKeeper needs to be specified as well to allow bootstrapping.

```json
    "ZookeeperBootComponent": {
      "m_path": "/dxram",
      "m_connection": {
        "m_ip": "127.0.0.1",
        "m_port": 2181
      }
```

### Storage size
To increase/decrease the ram size for storing chunks in DXRAM, modify the following entry under  "MemoryManagerComponent":
```json
"m_keyValueStoreSize": {
    "m_value": 134217728,
    "m_unit": "b"
}
```
Different units include "b", "kb", "mb", "gb" and "tb". Or add a vm argument when starting a DXRAM instance to override the value provided by the configuration, for example (128 MB):
```
-Ddxram.m_config.m_componentConfigs[MemoryManagerComponent].m_keyValueStoreSize.m_value=128
-Ddxram.m_config.m_componentConfigs[MemoryManagerComponent].m_keyValueStoreSize.m_unit=mb
```
