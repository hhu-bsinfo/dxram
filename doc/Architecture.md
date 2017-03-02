# DXRAM Architecture

DXRAM's core architecture is driven by a minimal foundation called the *DXRAMEngine*. All features that form the DXRAM system are encapsuled in either *components* and *services*.

TODO include arch image

## DXRAM Engine

The *DXRAMEngine* class provides a minimal foundation to run *components* and *services* which implement the actual functionality for the DXRAM system. 
The engine bootstrap using a JSON formated configuration file. A list of components and services (including their settings) are loaded into an instance of *DXRAMContext*. This allows enabling/disabling of components or services to configure a DXRAM instance to your needs. Configuration parameters for components or services are embedded with each class.

After bootstrapping with a configuration file, DXRAM first initializes all components using a fixed order which ensures resolving component dependencies correctly. Next, all services are initialized and the boot sequence is terminated by entering the main DXRAM application loop in the *DXRAMMain* class.

## Components

Components are the core building blocks for DXRAM and are based on the abstract class *AbstractDXRAMComponent*. They provide core functionalities such as a network interface (*NetworkComponent*), memory management (*MemoryManagerComponent*) or bootstrapping (*ZookeeperBootComponent*). Components can rely on other components as dependencies (e.g. *MemoryManagerComponent* requires *AbstractBootComponent*) and are allowed to access and use each other's features. However, a component is limited to provide functionality to the current DXRAM instance, only, e.g. a component is not allowed to handle communication or network packages with other DXRAM instances. This is done using *services*

## Services

Services are using components to establish an API for the application programmer to access the backend as well handle network communication with other DXRAM instances, e.g. access to the key-value store (*ChunkService*) or using the network interface to send your own messages (*NetworkService*). A service can use all components provided by DXRAM. But, services are isolated and not allowed to rely or use other services. This allows us to enable/disable services we don't need on a DXRAM instance and keeps the API highly modular and flexible. Furthermore, one can lower the resource requirements of a DXRAM instance by switching off services that are not used.

## 

# DXCompute

DXCompute is a layer built on top of DXRAM adding services to execute computations locally and remotely on peers. Applications can run computations on storage nodes (peers) benefiting from locally available data.

## JobService

The *JobService* allows user applications to spawn lightweight jobs on a peer node. A custom job has to implement the abstract class *AbstractJob*.

## MasterSlaveComputeService

The *MasterSlaveService* implements compute groups within the DXRAM network topology consisting of one coordinator (master) and an arbitrary number of compute nodes (slaves). The master node controls the slave nodes of its group by managing joining/leaving of slaves to the compute group, accepting compute tasks, scheduling compute tasks to all slaves and synchronizing slaves between compute tasks.

### Compute tasks

Tasks are submitted using the *MasterSlaveComputeService* and can be submitted either locally (if running on a peer) or to a remote peer. To write your own tasks, implement the *Task* interface. 

Multiple tasks can be aggregated to form a task script. A task script is a series of tasks executed in order. A number of common tasks that can be reused for many scripts are already available such as *PrintTask*, *EmptyTask*, *WaitTask* etc.

### Micro benchmarks

The subpackage *bench* contains tasks used for benchmarking various components/services of DXRAM. Refer to the each task for a description and the available parameters. Furthermore, the subfolder *script/compute* contains task scripts that can be executed using the terminal (refer to the readme).

# DXGraph

TODO