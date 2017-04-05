# Directories

Various compute scripts to run with the MasterSlaveComputeService. You can start these scripts from the DXRAM terminal using the *comptaskscript* command. Pay attention to the requirements of each script (number of superpeers/peers, memory per peer etc).

* [bench](bench/README.md): Scripts to run built in benchmarks on DXRAM
* example: Example scripts/templates for creating your own compute task scripts
* test: Scripts for testing different modules of the DXRAM system

# Running compute task scripts

A sample configuration on how to deploy DXRAM with DXCompute, the required MasterSlaveComputeService can be found in the *script/deploy/conf/example/SimpleComputeGroupTest.conf*. That config deploys a superpeer, two peers (one master and one slave) and starts a terminal. If further slaves are required make sure to extend this configuration script with further DXRAM instances that run as compute slaves.

A compute task script can deployed to a compute group from the DXRAM terminal using the *comptaskscript* command (see also command usage with *help comptaskscript*).

Ensure to meet the requirements of each task script you run (number of nodes, storage peer memory size etc).

## Enabling DXCompute

Check if available and otherwise add the following item to the *m_services* list in your DXRAM configuration, e.g. *config/dxram.json* to enable the MasterSlaveComputeService which is required to run the task scripts:
```
"MasterSlaveComputeService": {
  "m_class": "de.hhu.bsinfo.dxram.ms.MasterSlaveComputeService",
  "m_enabled": true
}
```

If the *dxram.json* file is not available, start the deploy script once to auto generate a default config file.

## Deployment and running an example script

Deploy DXRAM using the deploy script with the *SimpleComputeGroupTest.conf* config (execution directory is the dxram root dir):
```
./script/deploy/deploy.sh ./script/deploy/conf/example/SimpleComputeGroupTest.conf
```

In the terminal, run the following command:
```
comptaskscript script/compute/example/SimpleTaskScriptExample.cts 0
```

If everything's good, you should get some output about task submission and a return code of each slave that finished the task.
