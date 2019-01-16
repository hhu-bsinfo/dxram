# Requirements
DXRAM requires Java 1.8 to run on a Linux distribution of your choice (MacOSX does not work for deploying).
DXRAM uses [ZooKeeper](https://zookeeper.apache.org/) for bootstrapping.

## Dependencies

DXRAM requires a few dependencies such as ZooKeeper, gson or Log4j. The build system downloads them automatically.

Furthermore, DXRAM uses [DXNet](https://github.com/hhu-bsinfo/dxnet), [DXUtils](https://github.com/hhu-bsinfo/dxutils),
[DXMon](https://github.com/hhu-bsinfo/dxmon), [DXMem](https://github.com/hhu-bsinfo/dxmem) and
[DXLog](https://github.com/hhu-bsinfo/dxlog) which, by default, are also downloaded. If you prefer to use a local copy
of them (for development), clone the repository and place it next to the dxram root folder (same directory level).
The build system automatically detects it (if you don't change the folder's name) and compile the project
with your local copy instead.

# ZooKeeper
Download, install and setup [ZooKeeper](http://mirror.netcologne.de/apache.org/zookeeper/zookeeper-3.4.13/zookeeper-3.4.13.tar.gz).
See System Requirements in the admin guide of ZooKeeper.

# Building and install
Our build system uses gradle and the project comes with a gradle-wrapper included, so you don't have to install gradle.
Use the *build.sh* script to compile a release build of the project. Specify *debug* or *release* as an additional
argument, e.g. *build.sh debug* for different build types. Grab the *dxram.zip* from *build/dist and unpack it to a
location of your choice to install it.

# Starting DXRAM - Deployment
To easily deploy instances to either localhost or nodes of a cluster, we recommend using our deployment system
[cdepl](https://github.com/hhu-bsinfo/cdepl). Checkout the repository for example scripts for deploying DXRAM and
documentation about the deployment process.

However, you can also run dxram using the script *bin/dxram* from the *build/dist* output. Further instructions on how
to run DXRAM manually are provided [here](ManualSetup.md).

## Running applications on DXRAM
To run applications on DXRAM, please refer to [this document](Applications.md).

# InfiniBand
If you want to use InfiniBand instead of Ethernet network:
* You need a cluster with InfiniBand hardware and software stack installed
* Follow the instructions in the [*ibdxnet* repository](https://github.com/hhu-bsinfo/ibdxnet) to compile the
*JNIIbdxnet* library.
* Put the *libJNIIbdxnet.so* file into the *jni* subfolder on your deployed dxram instance(s)
* Open the *dxram.conf* and enable InfiniBand
(under *NetworkComponentConfig/m_core*):
```
"m_device": "Infiniband"
```
* Deploy your instances

Note: Depending on how you set up your environment, DXRAM/Ibdxnet needs permissions to pin memory
(capability: CAP_IPC_LOCK).
