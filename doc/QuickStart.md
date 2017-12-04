# Requirements
DXRAM requires Java 1.8 to run on a Linux distribution of your choice
(MacOSX should work as well but is not supported, officially).
DXRAM uses [ZooKeeper](https://zookeeper.apache.org/) for bootstrapping.

## Dependencies
The following dependencies are required and are already included with
DXRAM in the "lib" folder:
* ant-contrib (0.6) for building
* AutoValue (1.4.1) for DXRAM
* gson (2.7) for DXRAM
* jline (2.15) for DXRAM
* Log4j (version 1) for Zookeeper
* Log4j2 (2.7) for DXRAM
* sl4j (version 1) for Zookeeper
* sl4j-log4j12 for Zookeeper
* ZooKeeper (3.4.3) for DXRAM

DXRAM uses submodules like dxutils and dxnet. Make sure to load them as well after cloning:
```
git submodule update --init --recursive
```

# ZooKeeper
Download, install and setup [ZooKeeper](https://zookeeper.apache.org/).
See System Requirements in the admin guide of ZooKeeper (a packaged
version is included with DXRAM, see *util/zookeeper.zip*)

# Building
A build script (Ant) is included for building the project. Ensure Ant
is installed and run *build.sh*. The build output is located in
the *build/* sub-directory.

# Deploying to a remote machine (cluster)
Copy the *build/dxram* directory to your cluster.

# Starting DXRAM - Deployment
A bash script to easily deploy instances to either localhost or nodes
of a cluster is included (*script/deploy/deploy.sh*). We recommend running
the *env.sh* script from the DXRAM root which sets up environment variables.
This allows you to run the deploy script using the command *dxram-deploy*
without having to care about the script's location.
The script supports three modes of deployment.

## Demo mode
Demo mode is perfect to quickly start up one superpeer and one peer on the
current machine. You can try out the system without needing a cluster and going
through the process of writing your own configuration file(s). Just run
deployment without any parameters:
```
dxram-deploy
```
The minimum requirements are 4 GB of RAM (free) and a dual core CPU. The peer
deployed provides 1 GB of key-value storage.

## Simple mode
With simple mode, you can quickly deploy a default DXRAM setup to multiple
remote (cluster) nodes. Again, this doesn't need a configuration file. Just
add a list of hostnames to the deploy command to deploy one instance per node:
```
dxram-deploy node65 node66 node67
```
The example deploys one superpeer to node65 and one peer each to node66 and
node67. The amount of memory assigned is calculated based on the available
memory of the remote nodes.

## Configuration file mode
This mode gives you great flexibility and various options to tweak your
deployment of DXRAM. The script parses a configuration file and starts the
specified DXRAM instances. Example configuration files are located in the
sub-folders *script/deploy/conf* (getting started config: *SimpleTest.conf*).
For further details on deployment refer to
[this readme](../script/deploy/README.md).

To run a minimal DXRAM setup using a configuraiton file, compile DXRAM using
the *build.sh* script. Adjust the path at the top of the *SimpleTest.conf* and
run the deploy script with the *SimpleTest.conf* from the root of the DXRAM
folder:
```
./build.sh
./env.sh
dxram-deploy ./script/deploy/conf/example/SimpleTest.conf
```

The example starts zookeeper, one superpeer and one peer, locally. Any
log output of the instances is written to log files in a directory
called *deploy_log/xxx/logs*. If you encounter any errors, check out
the log files of each instance first.

## Cleanup/Kill deployment
Using the command *dxram-killall* you can cleanup the instances you started
using the above deployment modes.
```
# Kill all local instances (current machine)
dxram-killall

# Kill all remote instances on the declared nodes
dxram-killall node65 node66 node67

# Kill all intsances declared in the configuration file
dxram-killall ./script/deploy/conf/example/SimpleTest.conf
```

## Running terminal client

Once the deployment completed, you can start a terminal client to
connect to a DXRAM peer running a terminal server using the
*dxterm-client* script located in the DXRAM root folder:
```
dxterm-client localhost
```

Replace *localhost* with the hostname or IP address (IPV4) of a
running DXRAM peer. Now you can start experimenting with the system
by using [various terminal commands](Terminal.md).

# InfiniBand
If you want to use InfiniBand instead of Ethernet network:
* You need a cluster with InfiniBand hardware and software stack
installed
* Follow the instructions in the
[*ibdxnet* repository](https://github.com/hhu-bsinfo/ibdxnet) to
compile the *JNIIbdxnet* library.
* Put the *libJNIIbdxnet.so* file into the *jni* subfolder on your
deployed dxram instance(s)
* Open the *dxram.conf* and enable InfiniBand
(under *NetworkComponentConfig/m_core*):
```
"m_device": "Infiniband"
```
* Deploy your instances

Note: Depending on how you set up your environment, DXRAM/Ibdxnet needs
permissions to pin memory (capability: CAP_IPC_LOCK). Using our deploy
script, add the parameter *root=1* to your deploy config to run an
instance as root, e.g.:
```
node01,S,root=1
node02,P,root=1
```
