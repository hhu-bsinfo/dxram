# Requirements
DXRAM requires Java 1.8 to run on a Linux distribution of your choice (MacOSX should work as well but is not supported, officially). DXRAM uses [ZooKeeper](https://zookeeper.apache.org/) for bootstrapping.

## Dependencies
The following dependencies are required and are already included with DXRAM in the "lib" folder:
* ant-contrib (0.6) for building
* AutoValue (1.4.1) for DXRAM
* gson (2.7) for DXRAM
* jline (2.15) for DXRAM
* Log4j (version 1) for Zookeeper
* Log4j2 (2.7) for DXRAM
* sl4j (version 1) for Zookeeper
* sl4j-log4j12 for Zookeeper
* ZooKeeper (3.4.3) for DXRAM

# ZooKeeper
Download, install and setup [ZooKeeper](https://zookeeper.apache.org/). See System Requirements in the admin guide of ZooKeeper (a packaged version is included with DXRAM, see *util/zookeeper.zip*)

# Building
A build script (Ant) is included for building the project. Ensure Ant is installed and run *build.sh*. The build output is located in the *build/* sub-directory.

# Deploying to a remote machine (cluster)
Copy the *build/dxram* directory to your cluster.

# Starting DXRAM
A bash script to easily deploy instances to either localhost or nodes of a cluster is included (*script/deploy/deploy.sh*). The script parses a configuration file and starts the specified DXRAM instances. For examples, refer to the configurations in the sub-folders *script/deploy/conf*, especially the *examples* category with *SimpleTest.conf*. For further details on deployment refer to [this readme](../script/deploy/README.md).

Ensure that all scripts used (subfolder *script/deploy* and *script/deploy/modules*) have the executable bit set. You can also run the *env.sh* script to start an environment that sets these bits automatically.

To run a minimal DXRAM setup, compile DXRAM using the *build.sh* script. Adjust the paths at the top of the *SimpleTest.conf* and run the deploy script with the *SimpleTest.conf* from the root of the DXRAM folder:
```
./build.sh
./script/deploy/deploy.sh ./script/deploy/conf/example/SimpleTest.conf
```
The example starts zookeeper, one superpeer and one peer, locally. Any log output of the instances is written to log files in a directory called *xxx_deploy_tmp/logs*. If you encounter any errors, check out the log files of each instance first.

You can also run the *env.sh* script from the DXRAM root to setup environment variables. This allows you to deploy scripts without typing the path for *deploy.sh*:
```
dxram-deploy SimpleTest.conf
```
...and cleanup the deployed cluster:
```
dxram-killall SimpleTest.conf
```

Once the deployment completed, you can start a terminal client to connect to a DXRAM peer running a terminal server using the *dxterm-client* script located in the DXRAM root folder:
```
dxterm-client localhost
```

Replace *localhost* with the hostname or IP address (IPV4) of a running DXRAM peer. Now you can start experimenting with the system by using [various terminal commands](Terminal.md).
