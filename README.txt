1 Requirements
DXRAM needs Java 1.6 to run properly.

1.1 ZooKeeper
See System Requirements in the Admin guide of ZooKeeper.

2 Configuration
DXRAM is configured through a configuration file. By default the configuration file is named
dxram.config and can be found under the folder config. You can define another file by
setting the system propery dxram.config.file.
The config file should contain at least the local IP address (Do not use "localhost",
"127.0.0.1", etc. as IP address). For example

$network
dxram.ip=192.168.0.1

If you are using ZooKeeper for the DHTInterface you must add another line.

$zookeeper
zookeeper.connectionString=192.168.0.1:2181

In addition you should define the size of the used ram and log file.

$ram
dxram.ramsize=1073741824
$log
dxram.logfilesize=10737418240

The log file will be created under /home/dxram/log. You should create the path "/home/dxram" and
give write access to DXRAM or modify the log file position with

$log
dxram.logfile=/home/dxram/log
 
The file config/dxram_default.config gives an overview of all config parameters.

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

