# Requirements
DXRAM requires Java 1.8 to run on a Linux distribution of your choice (MacOSX might work as well but is not supported,
officially). DXRAM uses [ZooKeeper](https://zookeeper.apache.org/) for bootstrapping.

## Dependencies

DXRAM requires a few dependencies such as ZooKeeper, gson or Log4j. The build system downloads them automatically.

Furthermore, DXRAM uses [DXNet](https://github.com/hhu-bsinfo/dxnet), [DXUtils](https://github.com/hhu-bsinfo/dxutils),
[DXMon](https://github.com/hhu-bsinfo/dxmon) and [DXMem](https://github.com/hhu-bsinfo/dxmem) which, by default, are
also downloaded. If you prefer to use a local copy of them (for development), clone the repository and place it next to
the dxram root folder (same directory level). The build system automatically detects it's presence (if you don't change
the folder's name) and compile the project with your local copy instead.

# ZooKeeper
Download, install and setup [ZooKeeper](https://zookeeper.apache.org/). See System Requirements in the admin guide of
ZooKeeper (a packaged version is included with DXRAM, see *util/zookeeper.zip*).

# Building and install
Our build system uses gradle and the project comes with a gradle-wrapper included, so you don't have to install gradle.
For convenience, run the *build.sh* script in the root directory to start the build process. By specifying one of the
additional arguments *debug*, *release* or *performance*, you can easily build the different build types offered. The
default build type is *release*. The output with all necessary assets to run DXRAM is located in the *build/dist*
directory. Grab the *dxram* folder or *dxram.zip* and unpack it to a location of your choice to install it.

# Starting DXRAM - Deployment
To easily deploy instances to either localhost or nodes of a cluster, we recommend using our deployment system
[cdepl](https://github.com/hhu-bsinfo/cdepl). Checkout the repository for example scripts for deploying DXRAM and
documentation about the deployment process.

However, you can also run dxram using the script *bin/dxram* from the *build/dist* output. Further instructions on how
to run DXRAM manually are provided [here](ManualSetup.md).

## Running terminal client
Once deployment completed, you can start a terminal client to connect to a DXRAM peer running a terminal server
using the *dxterm-client* script located in the DXRAM root folder:
```
dxterm-client localhost
```

Replace *localhost* with the hostname or IP address (IPV4) of a running DXRAM peer. Now you can start experimenting
with the system by using [various terminal commands](Terminal.md).

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
