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

project(JNIIbdxnet)
message(STATUS "Project " ${PROJECT_NAME})

include_directories(${IBNET_LIBS_DIR})
include_directories(${IBNET_SRC_DIR})

include_directories(/usr/lib/jvm/java-8-openjdk/include/)
include_directories(/usr/lib/jvm/java-8-openjdk-amd64/include/)
include_directories(/usr/lib/jvm/java-8-openjdk/include/linux)
include_directories(/usr/lib/jvm/java-8-openjdk-amd64/include/linux)

set(SOURCE_FILES
        ${IBNET_SRC_DIR}/ibnet/dx/ConnectionCreator.cpp
        ${IBNET_SRC_DIR}/ibnet/dx/ConnectionHandler.cpp
        ${IBNET_SRC_DIR}/ibnet/dx/DebugThread.cpp
        ${IBNET_SRC_DIR}/ibnet/dx/JNIIbdxnet.cpp
        ${IBNET_SRC_DIR}/ibnet/dx/RecvBufferPool.cpp
        ${IBNET_SRC_DIR}/ibnet/dx/RecvHandler.cpp
        ${IBNET_SRC_DIR}/ibnet/dx/RecvThread.cpp
        ${IBNET_SRC_DIR}/ibnet/dx/SendBuffers.cpp
        ${IBNET_SRC_DIR}/ibnet/dx/SendHandler.cpp
        ${IBNET_SRC_DIR}/ibnet/dx/SendThread.cpp)

add_library(${PROJECT_NAME} SHARED ${SOURCE_FILES})

target_link_libraries(${PROJECT_NAME} IbnetCore)

set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fpic -rdynamic -g -O3 -fno-strict-aliasing -march=native")