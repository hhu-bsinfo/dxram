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

#include <cstdio>

#include "ibnet/sys/Logger.h"
#include "ibnet/sys/Network.h"

/**
 * Test network utility class
 */
int main(int argc, char** argv)
{
    ibnet::sys::Logger::Setup();

    std::string hostname = ibnet::sys::Network::GetHostname();
    std::printf("Own hostname: %s\n", hostname.c_str());
    std::printf("Own hostname ip: %s\n",
            ibnet::sys::Network::ResolveHostname(hostname).GetAddressStr().c_str());
    std::printf("IP for eth0: %s",
            ibnet::sys::Network::ResolveIPForInterface("eth0").GetAddressStr().c_str());

    return 0;
}