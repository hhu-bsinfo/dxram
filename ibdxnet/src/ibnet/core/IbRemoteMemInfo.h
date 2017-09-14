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

#ifndef IBNET_CORE_IBREMOTEMEMINFO_H
#define IBNET_CORE_IBREMOTEMEMINFO_H

#include <cstddef>
#include <cstdint>
#include <iostream>

namespace ibnet {
namespace core {

/**
 * Information about a remote node's allocated and pinned memory for RDMA
 * operations
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class IbRemoteMemInfo
{
public:
    /**
     * Constructor
     *
     * @param rkey Remote key for memory location
     * @param size Size of remote memory location
     */
    IbRemoteMemInfo(uint32_t rkey, uint32_t size) :
        m_rkey(rkey),
        m_size(size)
    {}

    /**
     * Destructor
     */
    ~IbRemoteMemInfo(void) {};

    /**
     * Get the remote key of the remote memory location
     */
    uint32_t GetRkey(void) const {
        return m_rkey;
    }

    /**
     * Get the size of the remote memory location
     */
    uint32_t GetSize(void) const {
        return m_size;
    }

    /**
     * Enable usage with out streams
     */
    friend std::ostream &operator<<(std::ostream& os,
            const IbRemoteMemInfo& o) {
        return os << "0x" << std::hex << o.m_rkey << ", " << o.m_size;
    }

private:
    uint32_t m_rkey;
    uint32_t m_size;
};

}
}

#endif // IBNET_CORE_IBREMOTEMEMINFO_H
