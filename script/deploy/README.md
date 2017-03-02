# Deployment script

The deployment script (*deploy.sh*) allows quick deployment of muliple DXRAM instances with minimal effort to a cluster or localhost for testing.

# How to deploy

A dedicated configuration file specifies the number of instances as well as parameters for each instance to run. Examples are provided in the subfolder *conf/examples*.

To execute deployment, run the deploy script with a configuration:
```
./deploy.sh conf/example/SimpleTest.conf
```

You can also run it from other locations as long as you have it located inside the *script/deploy* folder, e.g. from the DXRAM root folder:
```
./script/deploy/deploy.sh ./script/deploy/conf/example/SimpleTest.conf
````

# Configuration

## Execution paths

Deploying a minimal DXRAM setup is very easy and requires a few paramters, only. Refering to the *conf/example/SimpleTest.conf*, two paths must be specified at the top of the configuration
```
DXRAM_PATH=../../../../
ZOOKEEPER_PATH=~/zookeeper/
```

Relative paths (relative to the location of the configuration file) as well as ~ to reference the current user's home are supported as well as absolute paths. Make sure *DXRAM_PATH* points to the root *dxram* folder and *ZOOKEEPER_PATH* to the root of your ZooKeeper installation.

## DXRAM instances/nodes (localhost)

Following the execution paths, every line denotes a single instance, for example
```
localhost,Z
```

The first parameter *localhost* specifies the location of the instance and the second parameter the type of instance to deploy to that location. Possible instance types are *Z* (ZooKeeper), *S* (DXRAM superpeer), *P* (DXRAM peer), *T* (DXRAM terminal). Further instance types might be added in the future and are located in the subfolder *modules*.

Refering to the *conf/example/SimpleTest.conf*, we deploy a minimal setup on localhost with one ZooKeeper instance, one DXRAM superpeer, peer and terminal. 

All instances specified are started sequentially, i.e. specifying the terminal at the end of the list will spawn it after all other instances have been started.

## DXRAM instances/nodes (cluster)

Instead of localhost, you can specify hostnames of cluster nodes that are resolved automatically by the deploy script on deployment, e.g.

```
node65,Z
node65,S
node66,P
node67,P
node65,T
```

## DXRAM instance parameters

Further parameters can be added by separating them with "," after specifying the location and instance type.

### Java VM options

Specify additional Java VM options (all instance types excluding ZooKeeper) separated by "^" that are added when running the instance, e.g. to increase heap and stack sizes of the JVM:
```
localhost,P,vmopts=Xms2048M^Xmx6144M
```

### Key-Value store size

Peers only. Specify the size of the key-value store in MB, e.g. 4 GB = 4096 MB:
```
localhost,P,kvss=4096
```

### Java program arguments

To pass normal program arguments to main (useful for custom applications built on top of DXRAM):
```
localhost,P,args=server 10
```

### Class to execute

To execute different DXRAM based applications, e.g. DXCompute, DXGraph or your own custom application built on DXRAM, you can specify the class to run:
```
localhost,P,class=de.hhu.bsinfo.dxram.run.beineke.MailboxTest
```

If not specified, it defaults to *de.hhu.bsinfo.dxram.run.DXRAMMain*.

### Wait for instance to finish startup

You can specify a string that needs to appear on the log before the instance is considered as started by the deploy script. The deploy script will start the next instance in order only if that string appeared on the log output.

This can be useful if you want to wait until certain tasks completed to ensure your system is fully up and operatable, e.g. loading of data, waiting for a server connection, etc. This is applicable for superpeers, peers and terminals.

```
localhost,P,cond=Server started
```

Instead of waiting for a specific string to print on the log, you can also wait for a specific amount of time (in seconds) until the instance is considered started:

```
localhost,P,tcond=5
```

Again, this is something you only need if you have developed your own custom application. 

### Auto start a script (terminal only)

You can execute a Java script file written for the DXRAM terminal environment after the terminal has started. This can be very useful to run benchmarks or other automated tasks after all other DXRAM instances have started. The terminal will not execute any scripts if not specified explicitly.

```
localhost,T,OverlayTest.js
```

Make sure to have the terminal instance placed as the last instance to deploy in your configuraiton.