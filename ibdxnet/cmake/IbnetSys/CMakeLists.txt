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

project(IbnetSys)
message(STATUS "Project " ${PROJECT_NAME})

include_directories(${IBNET_LIBS_DIR})
include_directories(${IBNET_SRC_DIR})

set(SOURCE_FILES
        ${IBNET_SRC_DIR}/ibnet/sys/AddressIPV4.cpp
        ${IBNET_SRC_DIR}/ibnet/sys/Logger.cpp
        ${IBNET_SRC_DIR}/ibnet/sys/Network.cpp
        ${IBNET_SRC_DIR}/ibnet/sys/Random.cpp
        ${IBNET_SRC_DIR}/ibnet/sys/SocketUDP.cpp
        ${IBNET_SRC_DIR}/ibnet/sys/StringUtils.cpp)

add_library(${PROJECT_NAME} STATIC ${SOURCE_FILES})

target_link_libraries(${PROJECT_NAME} dl dw)

set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -rdynamic -g -fpic -O3 -fno-strict-aliasing -march=native")