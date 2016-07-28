#!/bin/bash

java \
-Ddxram.config=config/dxram.default.conf \
-Ddxram.config.0=config/dxram.nodes.local.conf \
-Ddxram.config.val.0=/DXRAMEngine/Settings/IP#str#127.0.0.1 \
-Ddxram.config.val.1=/DXRAMEngine/Settings/Port#int#22220 \
-Ddxram.config.val.2=/DXRAMEngine/Settings/Role#str#Terminal \
-cp lib/slf4j-log4j12-1.6.1.jar:lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/log4j-1.2.16.jar:bin/ \
de.hhu.bsinfo.dxram.run.DXRAMMain
