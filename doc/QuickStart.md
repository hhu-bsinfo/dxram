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
is installed and run *build.sh*. The ant scripts used for building are
located in the subfolder *ant/*. The build output is located in
the *build/* sub-directory.

# Deploying to a remote machine (cluster)
Copy the *build/dxram* directory to your cluster.

# Starting DXRAM - Deployment
To easily deploy instances to either localhost or nodes of a cluster,
we recommend using our deployment system
[cdepl](https://github.com/hhu-bsinfo/cdepl). Checkout the repository
for example scripts for deploying DXRAM and documentation about the
deployment process.

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
