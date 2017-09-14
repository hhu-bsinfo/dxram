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

#ifndef IBNET_SYS_ADDRESSIPV4_H
#define IBNET_SYS_ADDRESSIPV4_H

#include <cstdint>
#include <iostream>
#include <string>

namespace ibnet {
namespace sys {

/**
 * Class wrapping an IPV4 address (with port)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class AddressIPV4
{
public:
    static const uint32_t INVALID_ADDRESS = (const uint32_t) -1;
    static const uint16_t INVALID_PORT = (const uint16_t) -1;

    /**
     * Default constructor
     *
     * Inits members to invalid
     */
    AddressIPV4(void);

    /**
     * Constructor
     *
     * Port is set to invalid
     *
     * @param address Address to set
     */
    AddressIPV4(uint32_t address);

    /**
     * Constructor
     *
     * @param address Address to set
     * @param port Port to set
     */
    AddressIPV4(uint32_t address, uint16_t port);

    /**
     * Constructor
     *
     * @param address Address as string, e.g. 127.0.0.1 or 127.0.0.1:12345
     */
    AddressIPV4(const std::string& address);

    /**
     * Destructor
     */
    ~AddressIPV4(void);

    /**
     * Get the address
     */
    uint32_t GetAddress(void) const {
        return m_address;
    }

    /**
     * Get the port
     */
    uint16_t GetPort(void) const {
        return m_port;
    }

    /**
     * Check if the address is invalid (-1)
     *
     * @return True invalid, false otherwise
     */
    bool IsValid(void) {
        return m_address != INVALID_ADDRESS;
    }

    /**
     * Get the address as a string, e.g. "127.0.0.1"
     *
     * @param withPort True to add the port, e.g. "127.0.0.1:12345"
     * @return Address (with port) as string
     */
    const std::string GetAddressStr(bool withPort = false) const {
        if (withPort) {
            return m_addressStr + ":" + std::to_string(m_port);
        } else {
            return m_addressStr;
        }
    }

    /**
     * Enable usage with out streams
     */
    friend std::ostream &operator<<(std::ostream& os, const AddressIPV4& o) {
        return os << o.m_addressStr;
    }

private:
    uint32_t m_address;
    uint16_t m_port;
    std::string m_addressStr;

    void __ToString(uint32_t address);
    void __ToAddressAndPort(const std::string& address);
};

}
}

#endif //IBNET_SYS_ADDRESSIPV4_H
