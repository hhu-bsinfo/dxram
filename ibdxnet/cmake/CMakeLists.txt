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

cmake_minimum_required(VERSION 3.5)

set(IBNET_ROOT_DIR "${CMAKE_CURRENT_LIST_DIR}/..")
set(IBNET_SRC_DIR "${IBNET_ROOT_DIR}/src")
set(IBNET_LIBS_DIR "${IBNET_ROOT_DIR}/libs")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")

# force out of source build
message(STATUS "Force out of source build check...")
string(COMPARE EQUAL "${CMAKE_SOURCE_DIR}" "${CMAKE_BINARY_DIR}" insource)
if (insource)
    message(FATAL_ERROR "Do not build in your source dir. Use a dedicated build folder. Now go cleanup the cache in the CMakeCache.txt and CMakeFiles folder which don't belong there!")
endif ()

# binary output dir
set(IBNET_BIN_OUTPUT_PATH "${IBNET_ROOT_DIR}/bin" CACHE STRING "Output path for compiled binaries")

# First for the generic no-config case (e.g. with mingw)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${IBNET_BIN_OUTPUT_PATH}/${OUTPUTCONFIG_PLATFORM} PARENT_SCOPE)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${IBNET_BIN_OUTPUT_PATH}/${OUTPUTCONFIG_PLATFORM} PARENT_SCOPE)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${IBNET_BIN_OUTPUT_PATH}/${OUTPUTCONFIG_PLATFORM} PARENT_SCOPE)
# Second, for multi-config builds (e.g. msvc)
foreach (OUTPUTCONFIG ${CMAKE_CONFIGURATION_TYPES})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${IBNET_BIN_OUTPUT_PATH}/${OUTPUTCONFIG_PLATFORM}/${OUTPUTCONFIG} PARENT_SCOPE)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${IBNET_BIN_OUTPUT_PATH}/${OUTPUTCONFIG_PLATFORM}/${OUTPUTCONFIG} PARENT_SCOPE)
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${IBNET_BIN_OUTPUT_PATH}/${OUTPUTCONFIG_PLATFORM}/${OUTPUTCONFIG} PARENT_SCOPE)
endforeach (OUTPUTCONFIG CMAKE_CONFIGURATION_TYPES)

#set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize=undefined -fsanitize=null -fsanitize=return -fsanitize=bounds -lasan -lubsan")
#set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=leak -fsanitize-recover=all")

add_subdirectory(IbConnectionManagerTest)
add_subdirectory(IbDeviceTest)
add_subdirectory(IbnetCore)
add_subdirectory(IbnetSys)
add_subdirectory(JNIIbdxnet)
add_subdirectory(NetworkTest)
add_subdirectory(SocketUdpTest)