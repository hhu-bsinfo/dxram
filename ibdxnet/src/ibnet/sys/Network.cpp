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

#include "Network.h"

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <string.h>
#include <unistd.h>

#include "ibnet/sys/SystemException.h"

namespace ibnet {
namespace sys {

const std::string Network::GetHostname(void)
{
    char buf[1024];

    if (gethostname(buf, sizeof(buf)) < 0) {
        throw SystemException("Buffer too small for getting own hostname");
    }

    std::string str(buf);

    return str;
}

AddressIPV4 Network::ResolveHostname(const std::string& hostname)
{
    struct hostent* he;
    char own_hostname[1024];

    if (gethostname(own_hostname, sizeof(own_hostname)) < 0) {
        throw SystemException("Getting own hostname failed");
    }

    he = gethostbyname(hostname.c_str());

    if (he == NULL) {
        throw SystemException("ResolveHostname for " + hostname + ": No such host");
    }

    if (he->h_addrtype != AF_INET) {
        throw SystemException("ResolveHostname (" + hostname +
                "): gethostbyname addrtype != AF_INET");
    }

    // exception for own hostname: we can't resolve that using DNS.
    // This leads to 127.0.1.1
    if (!strcmp(own_hostname, hostname.c_str())) {
        // assuming eth0 as default iface
        return ResolveIPForInterface("eth0");
    } else {
        return AddressIPV4(inet_ntoa(*((struct in_addr *) he->h_addr)));
    }
}

AddressIPV4 Network::ResolveIPForInterface(const std::string& iface)
{
    struct ifaddrs* addrs;
    struct ifaddrs* ptr;
    uint32_t ip = (uint32_t) -1;

    getifaddrs(&addrs);

    for (ptr = addrs; ptr; ptr = ptr->ifa_next) {

        if (ptr->ifa_addr->sa_family == AF_INET &&
                !strcmp(ptr->ifa_name, iface.c_str())) {
            struct sockaddr_in* addr_in = (struct sockaddr_in*) ptr->ifa_addr;

            ip = addr_in->sin_addr.s_addr;
            break;
        }
    }

    freeifaddrs(addrs);
    return AddressIPV4(ntohl(ip));
}

}
}