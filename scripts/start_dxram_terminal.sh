#!/bin/bash

java \
-Ddxram.config=config/dxram.student.conf \
-Ddxram.config.0=config/dxram.nodes.student.conf \
-Ddxram.network.ip=127.0.0.1 \
-Ddxram.network.port=22220 \
-Ddxram.role=Terminal \
-cp lib/slf4j-log4j12-1.6.1.jar:lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/log4j-1.2.16.jar:bin/ \
de.hhu.bsinfo.dxram.run.DXRAMMain
