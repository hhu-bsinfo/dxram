# Requirements
DXRAM requires Java 1.8 to run on a Linux distribution of your choice (MacOSX should work as well but is not supported, officially). DXRAM uses [ZooKeeper](https://zookeeper.apache.org/) for bootstrapping.

## Dependencies
The following dependencies are required and are already included with DXRAM in the "lib" folder:
* ant-contrib (0.6) for building
* gson (2.7) for DXRAM
* jline (1.0) for DXRAM
* Log4j (version 1) for Zookeeper
* Log4j2 (2.7) for DXRAM
* sl4j (version 1) for Zookeeper
* sl4j-log4j12 for Zookeeper
* zokeeper (3.4.3) for DXRAM

# ZooKeeper
Download, install and setup [ZooKeeper](https://zookeeper.apache.org/). See System Requirements in the admin guide of ZooKeeper.

# Building
An ant build script is included for building the project. Ensure ant is installed and run *build.sh*. The build output is located in the *build/* sub-directory.

# Deploying to a remote machine (cluster)
Copy the *build/dxram* directory to your cluster.

# Starting DXRAM
A bash script to easily deploy instances to either local host or nodes of a cluster is included (*script/deploy/deploy.sh*). The script parses a configuration file and starts the specified DXRAM instances. For examples, refer to the configurations in the sub-folders *script/deploy/conf*, especially the *examples* category with *SimpleTest.conf*. For further details on deployment refer to [this readme](script/deploy/README.md).

To run a minimal DXRAM setup, compile DXRAM using the *build.sh* script. Adjust the paths at the top of the *SimpleTest.conf* and run the deploy script with the *SimpleTest.conf* from the root of the DXRAM folder:
```
./build.sh
./script/deploy/deploy.sh ./script/deploy/conf/example/SimpleTest.conf
```
The example starts zookeeper, one superpeer, one peer and the terminal, locally. Any log output of the instances is written to log files in a directory called *xxx_deploy_tmp/logs*. If you encounter any errors, check out the log files of each instance first.
