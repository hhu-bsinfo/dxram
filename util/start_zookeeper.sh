#!/bin/bash

./zookeeper/bin/zkEnv.sh
./zookeeper/bin/zkServer.sh start
./zookeeper/bin/zkCli.sh