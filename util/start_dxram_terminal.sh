#!/bin/bash

java \
-Dlog4j.configurationFile=config/log4j.xml \
-Ddxram.config=config/dxram.json \
-Ddxram.m_engineSettings.m_address.m_ip=127.0.0.1 \
-Ddxram.m_engineSettings.m_address.m_port=22220 \
-Ddxram.m_engineSettings.m_role=Terminal \
-cp lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/gson-2.7.jar:lib/log4j-api-2.7.jar:lib/log4j-core-2.7.jar:lib/jline-1.0.jar:bin/ \
de.hhu.bsinfo.dxram.run.DXRAMMain
