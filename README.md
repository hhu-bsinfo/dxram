# DXRAM

## Requirements
DXRAM needs Java 1.8 to run properly. Currently supported operating 
systems are MacOSX and Linux. DXRAM uses zookeeper for bootstrapping 
(see subfolder tools/).

## Project structure
Clone the project and setup the project like this:
```
	workspace_dxram/
	->dxram/
	->zookeeper/
	->start_zookeeper.sh
```
You can unpack zookeeper from the subfolder tools/. The script is located
in the subfolder scripts/

### ZooKeeper
See System Requirements in the Admin guide of ZooKeeper.

## IDE configuration

### Eclipse
If you build from source with eclipse, use the configuration files in 
the folder eclipse/ for Eclipse IDE for Java Developers.

Open Eclipse and select a folder as your workspace 
(e.g. /home/Bob/Documents/workspace_dxram).
Your folder structure should look like this:
```	 
	workspace_dxram/
	->dxram/
		->bin/
		—>config/
		—>dxram-build.xml
		—>eclipse/
		—>jni/
		—>lib/
		—>README.txt
		—>scripts/
		—>src/
		—>tools/
```

Now switch back to Eclipse and create a new Java project. 
Name it "dxram" (name has to match dxram subfolder in workspace_dxram). 
Eclipse should detect the dxram folder in the workspace, automatically 
disable the configuration parameters and focus the finish 
button - press it.
Create two new run configurations with main class DXRAMMain. Add the
vm arguments from the next section for the superpeer and the peer.

### Configuration IntelliJ
Open IntelliJ and create a new project from existing sources. Choose the dxram folder from your workspace and click next until the creation process has finished.
IntelliJ should detect the used libraries automatically. In order you want to use the Run Configurations for Eclipse you
have to download the _eclipser_ plugin for IntelliJ. After you have downloaded the plugin just Right-Click the Configuration File
and hit _Convert with Eclipser_.

If you don't want to use eclipser proceed as follows:
Create two new Run Configurations in IntelliJ by creating them in the context menu under _Run > Edit Configuration ..._.
The first one should be named DXRAMMainPeer(22222) and should have following VM Options:
```
	-Ddxram.config=config/dxram.nothaas.conf
	-Ddxram.config.0=config/dxram.nodes.local.conf
	-Ddxram.config.val.0=/DXRAMEngine/Settings/IP#str#127.0.0.1
	-Ddxram.config.val.1=/DXRAMEngine/Settings/Port#int#22222
	-Ddxram.config.val.2=/DXRAMEngine/Settings/Role#str#Peer
    -Xmx1024M
```
The second one should be named DXRAMSuperpeer(22221) and should have following VM Options:
```
	-Ddxram.config=config/dxram.default.conf
	-Ddxram.config.0=config/dxram.nodes.local.conf
	-Ddxram.config.val.0=/DXRAMEngine/Settings/IP#str#127.0.0.1
	-Ddxram.config.val.1=/DXRAMEngine/Settings/Port#int#22221
	-Ddxram.config.val.2=/DXRAMEngine/Settings/Role#str#Superpeer
    -Xmx1024M
```
Don't forget choosing the DXRAMMain class as the main class.


## DXRAM Configuration
DXRAM is configured using one or multiple configuration files. 
To get started, the eclipse run configuration uses the configurations 
"dxram.student.conf" and "dxram.nodes.student.conf" in the run 
configuration parameters. Without any modification, the default 
configurations allow you to start one superpeer and one peer for local
testing.

### Nodes configuration file

Depending on your setup (local or cluster) you have to adjust the configuration
of the nodes. Every node needs its IP and a free port assigned. The nodes
configuration needs to be the same for all instances. The roles assignable
are superpeer (S) and peer (P). Terminals are not listed in the nodes configuration.

```xml
	<Nodes>
		<Node>
			<IP __id="0" __type="str">127.0.0.1</IP>
			<Port __id="0" __type="int">22221</Port>
			<Role __id="0" __type="str">S</Role>
			<Rack __id="0" __type="short">0</Rack>
			<Switch __id="0" __type="short">0</Switch>
		</Node>
		<Node>
			<IP __id="1" __type="str">127.0.0.1</IP>
			<Port __id="1" __type="int">22222</Port>
			<Role __id="1" __type="str">P</Role>
			<Rack __id="1" __type="short">0</Rack>
			<Switch __id="1" __type="short">0</Switch>
		</Node>
	</Nodes>
```
When adding more "Node" entries to the "Nodes" section, make sure to 
increment the "id" attribute and avoid having identical ids for multiple nodes.
This is necessary for the configuration to identify the entries correctly.

Furthermore, the IP and port of the node running zookeeper needs to be
specified as well to allow boostrapping.

```xml
	<ZookeeperBootComponent
		<ConnectionString __type="str">127.0.0.1:2181</ConnectionString>
	</ZookeeperBootComponent>
```

### DXRAM settings

All other settings are stored in the "normal" configuration file. There
are lots of variables for tweaking different components of the system
but for most applications you might have to adjust the following values 
only.

#### Storage size
To increase/decrease the ram size for storing data in DXRAM, modify the
following entry in said configuration under "ComponentSettings" by setting 
both RamSize and SegmentSize values to the desired total ram size:
```xml
	<MemoryManagerComponent>
		<KeyValueStoreSizeBytes __type="long" __unit="gb2b">1</KeyValueStoreSizeBytes>
	</MemoryManagerComponent>
```
Or add a vm argument override to the list on startup, for example (1 GB):
```
	-Ddxram.config=config/dxram.nothaas.conf
	-Ddxram.config.0=config/dxram.nodes.local.conf
	-Ddxram.config.val.0=/DXRAMEngine/Settings/IP#str#127.0.0.1
	-Ddxram.config.val.1=/DXRAMEngine/Settings/Port#int#22222
	-Ddxram.config.val.2=/DXRAMEngine/Settings/Role#str#Peer
	-Ddxram.config.val.3=/DXRAMEngine/ComponentSettings/MemoryManagerComponent/KeyValueStoreSizeBytes#long#1073741824
    -Xmx1024M
```

#### Logger and Debugging
The logger can be configured to output to either console or file. Both 
support limiting the output to different levels: disabled, error, warning, info, debug, trace.
```xml
	<Logger>
		<Level __type="str">debug</Level>
		<File>
			<Level __type="str">error</Level>
			<Path __type="str">log.txt</Path>
			<BackupOld __type="bool">false</BackupOld>
		</File>
		<Console>
			<Level __type="str">info</Level>
		</Console>
	</Logger>
```
You can adjust the global level (here debug) to limit all log output to 
a certain level or individual for either console (here info) or file (here error).
Again, this can be added as vm argument override in the same scheme as the key value store size.

## Test and Run
Execute the _start_zookeeper.sh_ in a terminal to start ZooKeeper. Now 
run the project in Eclipse via the run configurations. First you have to 
run the DXRAMSuperpeer, then the DXRamMainPeer. If you can’t find both 
possibilities in the Run History (_Run > Run History_) you have to double 
click DXRAMMainPeer and DXRAMSuperPeer in the Run Configuration (_Run > Run Configuration_).

## Build DXRAM as deployable jar
The Ant build-script "dxram-build.xml" creates a folder "build" under the 
project directory. The folder contains the jar-file for DXRAM and three 
additional folders "config", "jni" and "lib" needed to run the jar. 
The config-folder contains the configuration-files copied from config 
for dxram and the log4j-Logger, the lib-folder contains the libs used by dxram and
jni comes with compiled labraries for native access for some parts of dxram.

## Running the jar file
The jar-file can run the core DXRAM system. To run a superpeer locally use:
```bash
java -Ddxram.config=config/dxram.default.conf -Ddxram.config.0=config/dxram.nodes.local.conf -Ddxram.config.val.0=/DXRAMEngine/Settings/IP#str#127.0.0.1 -Ddxram.config.val.1=/DXRAMEngine/Settings/Port#int#22221 -Ddxram.config.val.2=/DXRAMEngine/Settings/Role#str#Superpeer -cp DXRAM.jar de.hhu.bsinfo.dxram.run.DXRAMMain 
```
To run the peer locally:
```bash
java -Ddxram.config=config/dxram.default.conf -Ddxram.config.0=config/dxram.nodes.local.conf -Ddxram.config.val.0=/DXRAMEngine/Settings/IP#str#127.0.0.1 -Ddxram.config.val.1=/DXRAMEngine/Settings/Port#int#22222 -Ddxram.config.val.2=/DXRAMEngine/Settings/Role#str#Peer -cp DXRAM.jar de.hhu.bsinfo.dxram.run.DXRAMMain 
```

When running this on a distributed setup (like a cluster), make sure to adjust the network addresses.

## Style guides
If you are using eclipse as your development environment we would like you to stick to our style guide.
In the eclipse subfolder, you can find a format template for eclipse, which
provides you with automatic code formating. Furthermore, we are using the
plugin Checkstyle to aid with further style rules. A configuration for Checkstyle
can be found in the eclipse subfolder.