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

#ifndef IBNET_SYS_NETWORK_H
#define IBNET_SYS_NETWORK_H

#include <string>

#include "AddressIPV4.h"

namespace ibnet {
namespace sys {

/**
 * Helper class for network related tasks
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class Network
{
public:
    /**
     * Get the currently set hostname of the system
     */
    static const std::string GetHostname(void);

    /**
     * Resolve a hostname to an IPV4 address
     *
     * @param hostname Hostname to resolve
     * @return If successful, a valid AddressIPV4 object, otherwise the
     *          returned object is set invalid.
     */
    static AddressIPV4 ResolveHostname(const std::string& hostname);

    /**
     * Resolve the name of a network interface to an IPV4 address
     *
     * @param iface Interface name to resolve
     * @return If successful, a valid AddressIPV4 object, otherwise the
     *          returned object is set invalid.
     */
    static AddressIPV4 ResolveIPForInterface(const std::string& iface);

private:
    Network(void) {};
    ~Network(void) {};
};

}
}

#endif //IBNET_SYS_NETWORK_H
