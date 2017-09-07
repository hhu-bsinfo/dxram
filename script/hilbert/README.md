# Deploy Script for HILBERT

This is deploy script wraps the normal one to allow deployment to the [HILBERT HPC cluster](https://www.zim.hhu.de/high-performance-computing.html). You use a configuration file that defines the node setup very similar to the normal deploy config for normal clusters.

# How to deploy

A dedicated configuration file specifies the number of instances as well as parameters for each instance to run. Examples are provided in the subfolder *conf/examples*.

To execute deployment, run the deploy script with a configuration:
```
./deploy_hilbert.sh conf/example/SimpleHilbertTest.conf
```

# Configuration

## Execution paths

Deploying a minimal DXRAM setup is very easy and requires a few parameters, only. Referring to the *conf/example/SimpleHilbertTest.conf*, two paths must be specified at the top of the configuration
```
DXRAM_PATH=/home/user/dxram
ZOOKEEPER_PATH=/home/user/zookeeper
```

These paths must be absolute and can't be relative like with the normal deploy script.

## HILBERT parameters

A list of parameters to configure the nodes and the job for hilbert are required:
```
HILBERT_NODE_COUNT=2
HILBERT_CPUS_PER_NODE=2
HILBERT_MEM_PER_NODE_GB=8
HILBERT_WALL_TIME=00:05:00
HILBERT_JOB_NAME=test
HILBERT_JOB_SUB_PARAMS=-I
```

The node count has to match the number of nodes specified afterwards. If you don't need the interactive mode, remove the *-I* parameter from *HILBERT_JOB_SUB_PARAMS*. This will drop you to a shell with all resources allocated for your job, once the scheduler has assigned them. You still have to start the job yourself (from the shell) but this allows you to debug and run DXRAM deployment multiple times without waiting for the resources (until your wall time runs out). The generated job file will be located on *scratch_gs*. The path to it is printed to the shell before you drop into interactive mode.

## DXRAM instances

Following the paths and HILBERT parameters, you have to specify a list of instances:
```
h0,Z,shellcmd=module load Java/1.8.0; module load gcc/6.1.0
h0,S,shellcmd=module load Java/1.8.0; module load gcc/6.1.0
h1,P,shellcmd=module load Java/1.8.0; module load gcc/6.1.0
```

We can't specify hostnames because the job system will assign them for us. Thus, you have to specify the nodes with *h0*, *h1*, ... with a maximum ID of *HILBERT_NODE_COUNT* - 1. You also have to add the specified *shellcmd* paramaeters. The specified modules are required to run DXRAM.

## Working directory

Once you deployed to HILBERT, a job is submitted to the job system. The working directory with the filled in job template file, DXRAM logs, configuration file and terminal stdout/sterr in a file is located in */scratch_gs/user/dxram_tmp*.

## Terminal

Simply run the terminal from the login node:
```
cd ~/dxram
./dxterm-client hilbert65
```

Replace *hilbert65* with one of the assigned hilbert nodes. You can check the log file names to get a list of nodes assigned.

If you are in interactive mode, you can run the terminal on one of the hilbert nodes. Ensure that you have loaded Java, first:
```
module load Java/1.8.0
```

Then run the terminal:
```
cd ~/dxram
./dxterm-client hilbert65
```