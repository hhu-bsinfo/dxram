0 Project structure
The recommended project structure looks like this:
	workspace/
	->dxram/
	->zookeeper/
	->start_zookeeper.sh

1 Requirements
DXRAM needs Java 1.8 to run properly.

1.01
Download the current version of dram.zip from the web server.

1.1 ZooKeeper
See System Requirements in the Admin guide of ZooKeeper.

1.2 Eclipse
In order you want to use the configuration files from the folder eclipse/ you should use Eclipse (Eclipse IDE for Java Developers).

2 Configuration
DXRAM is configured through a configuration file. By default the configuration file is named dxram.config and can be found under the folder config.

2.01 Preconfiguration (optional)
If you are using ZooKeeper you must add another XML tag to the dxram.node.student.conf file.
	<ZookeeperBootComponent
		<ConnectionString __type="str">127.0.0.1:2181</ConnectionString>
	</ZookeeperBootComponent>

In addition you should define the size of the used ram and log file in the dxram.student.conf .
The entry for the ram size could look like this (you can find this entry in the ComponentSettings):
	<MemoryManagerComponent>
		<RamSize __type="long" __unit="gb2b">1</RamSize>						<SegmentSize __type="long" __unit="gb2b">1</SegmentSize>
	</MemoryManagerComponent>
The entry for the log file could look like this (you can find this entry in the ServiceSettings XML tag):
	<LogService>
		<LogChecksum __type="bool">true</LogChecksum>
		<FlashPageSize __type="int" __unit="kb2b">4</FlashPageSize>
		<LogSegmentSize __type="int" __unit="mb2b">8</LogSegmentSize>
		<PrimaryLogSize __type="long" __unit="mb2b">256</PrimaryLogSize>
		<SecondaryLogSize __type="long" __unit="mb2b">256</SecondaryLogSize>
		<WriteBufferSize __type="int" __unit="mb2b">256</WriteBufferSize>
		<ReorgUtilizationThreshold __type="int">70</ReorgUtilizationThreshold>
	</LogService>

2.1 Configuration of Eclipse
Open Eclipse and choose a folder as your workspace (e.g. /home/Bob/Documents/Eclipse workspace).
Minimize Eclipse and open your file browser. Navigate to your Eclipse working directory and extract the dxram.zip file. Your folder structure should look like this:
	Eclipse Workspace/
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
Now switch back to Eclipse and create a new project and name it as dxram. Eclipse should automatically disable the configuration parameters and focus the finish button - press it. Minimize Eclipse again and extract the zookeeper.zip to your eclipse workspace. You can find this file in the folder dxram/tools/. Your folder structure should look like this:
	Eclipse Workspace/
	—>dxram/
		—>…
	—>zookeeper/
		—>…
	—>start_zookeeper.sh
Lastly you should import the run configuration file in Eclipse. Go to File > Import … > Run/Debug > Launch Configurations and click Next. In the next dialog select the eclipse directory from the dxram folder as the „From Directory:“ and check the eclipse folder on the left half of the split pane and click finish.

2.2 Test Run
Execute the start_zookeeper.sh in a terminal to start ZooKeeper. Now run the project in Eclipse. Firstly you have to run the DXRAMSuperpeer Application and after that the DXRamMainPeer. If you can’t find both possibilities in the Run History (Run > Run History) you have double click DXRAMMainPeer and DXRAMSuperPeer in the Run Configuration (Run > Run Configuration). Now you should find both entries in the Run History section.

3 Build
The Ant build-script creates a folder "build" under the project directory. The folder contains the
jar-file for DXRAM and two additional folders "config" and "lib". The config-folder contains the
configuration-files for dxram and the log4j-Logger, the lib-folder contains the libs used by dxram.

4 Run
The jar-file is designed to start a main-method from a class of the default package of dxram. For
this purpose the first command line parameter ist the explicit name of the class to execute.
For example, the command "java -jar DXRAM.jar Mailbox server 10" starts the Mailbox Test-Application
with the parameters "server 10".
Attention should be paid to the fact that only one DXRAM instance could be run per physical or virtual
machine.

