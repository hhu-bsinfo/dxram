# Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf,
# Institute of Computer Science, Department Operating Systems
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation, either version 3 of the License,
# or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>

project(IbnetCore)
message(STATUS "Project " ${PROJECT_NAME})

include_directories(${IBNET_LIBS_DIR})
include_directories(${IBNET_SRC_DIR})

set(SOURCE_FILES
        ${IBNET_SRC_DIR}/ibnet/core/IbCompQueue.cpp
        ${IBNET_SRC_DIR}/ibnet/core/IbConnection.cpp
        ${IBNET_SRC_DIR}/ibnet/core/IbConnectionCreatorSimple.cpp
        ${IBNET_SRC_DIR}/ibnet/core/IbConnectionManager.cpp
        ${IBNET_SRC_DIR}/ibnet/core/IbConnectionManagerJobQueue.cpp
        ${IBNET_SRC_DIR}/ibnet/core/IbDevice.cpp
        ${IBNET_SRC_DIR}/ibnet/core/IbNodeConf.cpp
        ${IBNET_SRC_DIR}/ibnet/core/IbNodeConfArgListReader.cpp
        ${IBNET_SRC_DIR}/ibnet/core/IbProtDom.cpp
        ${IBNET_SRC_DIR}/ibnet/core/IbQueuePair.cpp
        ${IBNET_SRC_DIR}/ibnet/core/IbRecvQueue.cpp
        ${IBNET_SRC_DIR}/ibnet/core/IbSendQueue.cpp
        ${IBNET_SRC_DIR}/ibnet/core/IbSharedRecvQueue.cpp)

add_library(${PROJECT_NAME} STATIC ${SOURCE_FILES})

target_link_libraries(${PROJECT_NAME} IbnetSys ibverbs pthread)

set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fpic -rdynamic -g -O3 -fno-strict-aliasing -march=native")