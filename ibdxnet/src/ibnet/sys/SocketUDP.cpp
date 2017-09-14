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

#include "SocketUDP.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include "ibnet/sys/Logger.hpp"
#include "ibnet/sys/SystemException.h"

namespace ibnet {
namespace sys {

SocketUDP::SocketUDP(uint16_t port) :
        m_port(port),
        m_socket(-1)
{
    struct sockaddr_in addr;

    m_socket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);

    if (m_socket == -1) {
        IBNET_LOG_ERROR("Opening UDP socket failed: {}", strerror(errno));
        throw SystemException("Opening UDP socket failed");
    }

    // set socket non blocking on receive
    int flags = fcntl(m_socket, F_GETFL, 0);
    fcntl(m_socket, F_SETFL, flags | O_NONBLOCK);

    memset(&addr, 0, sizeof(struct sockaddr_in));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(m_socket, (const sockaddr*) &addr, sizeof(addr)) == -1) {
        close(m_socket);
        IBNET_LOG_ERROR("Binding UDP socket to port {} failed: {}",
                        port, strerror(errno));
        throw SystemException(
                "Binding UDP socket to port " + std::to_string(port) +
                " failed");
    }

    IBNET_LOG_DEBUG("Opened UDP socket on port {}", port);
}

SocketUDP::~SocketUDP(void)
{
    close(m_socket);
}

ssize_t SocketUDP::Receive(void* buffer, size_t size, uint32_t* recvIpv4)
{
    ssize_t length;
    struct sockaddr_in recv_addr;
    socklen_t recv_addr_len = sizeof(struct sockaddr_in);

    memset(&recv_addr, 0, sizeof(struct sockaddr_in));

    *recvIpv4 = (uint32_t) -1;

    length = recvfrom(m_socket, buffer, size, 0, (struct sockaddr*) &recv_addr,
                      &recv_addr_len);

    if (length < 0) {
        if (errno == EAGAIN) {
            // no data for non blocking
            return 0;
        }

        IBNET_LOG_ERROR("Receiving data failed ({}): {}", length,
                      strerror(errno));
        return -1;
    }

    *recvIpv4 = ntohl(recv_addr.sin_addr.s_addr);

    return length;
}

ssize_t SocketUDP::Send(void* buffer, size_t size, uint32_t addrIpv4, uint16_t port)
{
    ssize_t length;
    struct sockaddr_in recv_addr;
    socklen_t recv_addr_len = sizeof(struct sockaddr_in);

    memset(&recv_addr, 0, sizeof(struct sockaddr_in));

    recv_addr.sin_family = AF_INET;
    recv_addr.sin_port = htons(port);
    recv_addr.sin_addr.s_addr = htonl(addrIpv4);

    length = sendto(m_socket, buffer, size, 0, (struct sockaddr*) &recv_addr,
            recv_addr_len);

    if (length != size) {
        IBNET_LOG_ERROR("Sending data failed ({}): {}", length,
                strerror((int) length));
        return length;
    }

    return length;
}

}
}