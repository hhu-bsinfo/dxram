# Requirements
DXRAM requires Java 1.8 to run on a Linux distribution of your choice
(MacOSX should work as well but is not supported, officially).
DXRAM uses [ZooKeeper](https://zookeeper.apache.org/) for bootstrapping.

## Dependencies

DXRAM requires a few dependencies such as ZooKeeper, gson or Log4j. The build system downloads them automatically.

Furthermore, DXRAM uses DXNet, DXUtils, DXMon and DXMem which, by default, are also downloaded. If you prefer to use a
local copy of them (for development), change set *isDevelop=true* in the *settings.gradle* file. You have to provide
local copies of the repositories listed in that file and put them on the same directory level as DXRAM's.

# ZooKeeper
Download, install and setup [ZooKeeper](https://zookeeper.apache.org/).
See System Requirements in the admin guide of ZooKeeper (a packaged
version is included with DXRAM, see *util/zookeeper.zip*)

# Building and install
Our build system uses gradle and the project comes with a gradle-wrapper included, so you don't have to install gradle.
Run *./gradlew installDist* to build the project with default settings. Use *-PbuildType=* to specify different build
types such as *debug* or *release*, e.g. *./gradlew installDist -PbuildType=release*. The output with all necessary
assets to run DXRAM is located in the *build* directory. Grab the *dxram.zip* and unpack it to a location of your
choice to install it.

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
