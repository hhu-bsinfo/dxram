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

#include "AddressIPV4.h"

#include <arpa/inet.h>

#include "ibnet/sys/StringUtils.h"
#include "ibnet/sys/SystemException.h"

namespace ibnet {
namespace sys {

AddressIPV4::AddressIPV4(void) :
    m_address(INVALID_ADDRESS),
    m_port(INVALID_PORT)
{
    __ToString(m_address);
}

AddressIPV4::AddressIPV4(uint32_t address) :
    m_address(address),
    m_port(INVALID_PORT)
{
    __ToString(m_address);
}

AddressIPV4::AddressIPV4(uint32_t address, uint16_t port) :
    m_address(address),
    m_port(port)
{
    __ToString(m_address);
}

AddressIPV4::AddressIPV4(const std::string& address)
{
    __ToAddressAndPort(address);
}

AddressIPV4::~AddressIPV4(void)
{

}

void AddressIPV4::__ToString(uint32_t address)
{
    m_addressStr += std::to_string((address >> 24) & 0xFF) + ".";
    m_addressStr += std::to_string((address >> 16) & 0xFF) + ".";
    m_addressStr += std::to_string((address >> 8) & 0xFF) + ".";
    m_addressStr += std::to_string(address & 0xFF);
}

void AddressIPV4::__ToAddressAndPort(const std::string& address)
{
    std::vector<std::string> tokens = StringUtils::Split(address, ":");

    if (tokens.size() > 2) {
        throw SystemException("Invalid address format: " + address);
    }

    if (tokens.size() == 0) {
        m_address = INVALID_ADDRESS;
        m_port = INVALID_PORT;
        return;
    }

    struct in_addr addr;

    if (!inet_pton(AF_INET, tokens[0].c_str(), &addr)) {
        throw SystemException("Invalid address format: " + address);
    }

    m_address = ntohl(addr.s_addr);
    m_addressStr = tokens[0];

    if (tokens.size() == 2) {
        m_port = (uint16_t) std::stoi(tokens[1]);
    }
}

}
}