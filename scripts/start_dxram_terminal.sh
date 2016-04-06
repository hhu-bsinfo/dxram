#!/bin/bash

java -Dnetwork.ip=127.0.0.1 -Dnetwork.port=22220 -Ddxram.role=Monitor -cp lib/slf4j-log4j12-1.6.1.jar:lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/log4j-1.2.16.jar:bin/ de.hhu.bsinfo.dxram.run.PeerTerminal