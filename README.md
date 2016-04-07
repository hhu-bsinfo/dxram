# DXRAM

## Project structure
The recommended project structure looks like this:
```
	workspace/
	->dxram/
	->zookeeper/
	->start_zookeeper.sh
```
## Requirements
DXRAM needs Java 1.8 to run properly. Currently supported operating systems are
MacOSX and Linux.

### DXRAM
Download the current version of dram.zip from the web server or clone the repository.

### ZooKeeper
See System Requirements in the Admin guide of ZooKeeper.

### Eclipse
If you build from source with eclipse, use the configuration files in the folder eclipse/ for Eclipse IDE for Java Developers.

## Configuration of Eclipse
Open Eclipse and select a folder as your workspace (e.g. /home/Bob/Documents/workspace).
Minimize Eclipse and open your file browser. Navigate to your Eclipse working directory and extract the dxram.zip file. Your folder structure should look like this:
```	 
	workspace/
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

Now switch back to Eclipse and create a new Java project. Name it "dxram". Eclipse should detect the dxram folder in the workspace, automatically disable the configuration parameters and focus the finish button - press it. Minimize Eclipse again and extract the zookeeper.zip to your eclipse workspace. You can find this file in the folder dxram/tools/. Your folder structure should be like this:
```
	Eclipse Workspace/
	—>dxram/
		—>…
	—>zookeeper/
		—>…
	—>start_zookeeper.sh
```
Finally, import the run configuration file in Eclipse in the dxram project. Go to _File > Import … > Run/Debug > Launch Configurations_ and click _Next_.
In the next dialog select the "eclipse" directory from the dxram folder as the „From Directory:“ and check the eclipse folder on the left half of the split pane and click finish.

## DXRAM Configuration
DXRAM is configured through one or multiple configuration files. To get started, the eclipse run configuration uses the configurations "dxram.student.conf" and "dxram.nodes.student.conf" in the run configuration parameters. Without any modification, the default configurations allow you to start one superpeer and one peer for testing the system.

### Settings
Settings for different components or services in DXRAM are available in the "dxram.student.conf".

#### Storage size
To increase/decrease the ram size for storing data in DXRAM, modify the
following entry in said configuration under "ComponentSettings" by setting both RamSize and SegmentSize values to the desired total ram size:
```xml
	<MemoryManagerComponent>
		<RamSize __type="long" __unit="gb2b">1</RamSize>						
		<SegmentSize __type="long" __unit="gb2b">1</SegmentSize>
	</MemoryManagerComponent>
```

#### Logger and Debugging
The logger can be configured to output to either console or file. Both support limiting the output to different levels: error, warning, info, debug, trace.
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
You can adjust the global level (here debug) to limit all log output to a certain level or individual for either console (here info) or file (here error).

### Nodes and Zookeeper
DXRAM uses Zookeeper for bootstrapping thus we require the address of zookeeper in the configuration. This is stored in a separate configuration 
(like "dxram.nodes.student.conf).
```xml
	<ZookeeperBootComponent
		<ConnectionString __type="str">127.0.0.1:2181</ConnectionString>
	</ZookeeperBootComponent>
```

A list of nodes used with DXRAM needs to be edited/created if your setup changes (like on a cluster):
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
When adding more "Node" entries to the "Nodes" section, make sure to increment the "id" attribute and avoid having identical ids for multiple nodes.
This is necessary for the configuration to identify the entries correctly.

### Test and Run
Execute the _start_zookeeper.sh_ in a terminal to start ZooKeeper. Now run the project in Eclipse via the run configurations. First you have to run the DXRAMSuperpeer, then the DXRamMainPeer. If you can’t find both possibilities in the Run History (_Run > Run History_) you have to double click DXRAMMainPeer and DXRAMSuperPeer in the Run Configuration (_Run > Run Configuration_).

## Build DXRAM as deployable jar
The Ant build-script "dxram-build.xml" creates a folder "build" under the project directory. The folder contains the
jar-file for DXRAM and three additional folders "config", "jni" and "lib" needed to run the jar. The config-folder contains the
configuration-files copied from config for dxram and the log4j-Logger, the lib-folder contains the libs used by dxram and
jni comes with compiled labraries for native access for some parts of dxram.

## Running the jar file
The jar-file can run the core DXRAM system. To run a superpeer locally use:
```bash
java -Ddxram.config=config/dxram-student.conf -Ddxram.config.0=config/dxram.nodes.student.conf -Ddxram.network.ip=127.0.0.1 -Ddxram.network.port=22221 -Ddxram.role=Superpeer -cp DXRAM.jar de.hhu.bsinfo.dxram.run.DXRAMMain 
```
To run the peer locally:
```bash
java -Ddxram.config=config/dxram-student.conf -Ddxram.config.0=config/dxram.nodes.student.conf -Ddxram.network.ip=127.0.0.1 -Ddxram.network.port=22222 -Ddxram.role=Peer -cp DXRAM.jar de.hhu.bsinfo.dxram.run.DXRAMMain 
```

When running this on a distributed setup (like a cluster), make sure to adjust the network addresses.