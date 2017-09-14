/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf,
 * Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

#include <chrono>
#include <cstdio>
#include <iostream>
#include <thread>

#include "ibnet/sys/AddressIPV4.h"
#include "ibnet/sys/Logger.h"
#include "ibnet/sys/SocketUDP.h"

static ibnet::sys::AddressIPV4 destAddr;
static bool isServer;

static bool ParseOptions(
        int argc,
        char** argv)
{
    if (argc < 4) {
        printf("Usage: %s <dest ip> <port> <server/client>\n", argv[0]);
        return false;
    }

    destAddr = ibnet::sys::AddressIPV4(std::string(argv[1]) + ":" +
            std::string(argv[2]));

    isServer = !strcmp(argv[3], "server");

    return true;
}

/**
 * Test UDP socket
 */
int main(int argc, char** argv)
{
    if (!ParseOptions(argc, argv)) {
        return -1;
    }

    ibnet::sys::Logger::Setup();

    ibnet::sys::SocketUDP* socket;

    if (!isServer) {
        socket = new ibnet::sys::SocketUDP(12345);
    } else {
        socket = new ibnet::sys::SocketUDP(destAddr.GetPort());
    }

    uint8_t buffer[8] = {1, 2, 3, 4, 5, 6, 7, 8};

    if (!isServer) {
        std::cout << "Sending data to " << destAddr << std::endl;

        while (true) {
            ssize_t length = socket->Send(buffer, sizeof(buffer),
                    destAddr.GetAddress(), destAddr.GetPort());

            if (length != sizeof(buffer)) {
                std::cout << "Sending data to " << destAddr << " failed: " <<
                          length << std::endl;
            }

            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    } else {
        std::cout << "Waiting to receive data on port " << destAddr.GetPort() <<
                std::endl;

        while (true) {
            uint32_t recvIp = -1;
            ssize_t length = socket->Receive(buffer, sizeof(buffer), &recvIp);

            if (length > 0) {
                std::cout << "Received data from 0x" << std::hex <<
                          recvIp << ", length " << length << std::endl;

                for (ssize_t i = 0; i < length; i++) {

                    if (i >= sizeof(buffer)) {
                        break;
                    }

                    printf("0x%02X ", buffer[i]);
                }
                std::cout << std::endl;
            } else {
                std::cout << "Receiving data on port " << destAddr.GetPort() <<
                     " failed: " << length << std::endl;
            }

            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    delete socket;

    return 0;
}