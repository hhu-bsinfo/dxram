#!/bin/bash

java \
-Ddxram.config=config/dxram.json \
-Ddxram.m_engineSettings.m_address.m_ip=127.0.0.1 \
-Ddxram.m_engineSettings.m_address.m_port=22220 \
-Ddxram.m_engineSettings.m_role=Terminal \
-cp lib/slf4j-log4j12-1.6.1.jar:lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/log4j-1.2.16.jar:lib/gson-2.7.jar:bin/ \
de.hhu.bsinfo.dxram.run.DXRAMMain
