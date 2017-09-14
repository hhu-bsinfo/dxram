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

#ifndef IBNET_CORE_IBREMOTEINFO_H
#define IBNET_CORE_IBREMOTEINFO_H

#include <cstdint>
#include <iostream>

#include "IbNodeId.h"

namespace ibnet {
namespace core {

/**
 * Information about a remote node which is required to setup a connection/
 * queue pair
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class IbRemoteInfo
{
public:
    /**
     * Constructor
     *
     * Sets invalid node info
     */
    IbRemoteInfo(void) :
            m_nodeId(IbNodeId::INVALID),
            m_lid((uint16_t) -1)
    {}

    /**
     * Constructor
     *
     * @param nodeId Node id of the remote node
     * @param lid LID of the remote node
     * @param conManIdent Identifier of connection manager to detect
     *      rebooted nodes
     * @param physicalQpIds Physical QP ids of the remote QPs to connect to
     */
    IbRemoteInfo(uint16_t nodeId, uint16_t lid, uint32_t conManIdent,
            const std::vector<uint32_t>& physicalQpIds) :
        m_nodeId(nodeId),
        m_lid(lid),
        m_conManIdent(conManIdent),
        m_physicalQpIds(physicalQpIds)
    {}

    /**
     * Destructor
     */
    ~IbRemoteInfo(void)
    {}

    /**
     * Check if the remote info is valid
     */
    bool IsValid(void) const {
        return m_nodeId != IbNodeId::INVALID;
    }

    /**
     * Get the node id of the remote node
     */
    uint16_t GetNodeId(void) const {
        return m_nodeId;
    }

    /**
     * Get the LID of the remote node
     */
    uint16_t GetLid(void) const {
        return m_lid;
    }

    /**
     * Get the identifier of the remote connection manager
     */
    uint32_t GetConManIdent(void) const {
        return m_conManIdent;
    }

    /**
     * Get the list of physical QP ids of the remote node
     */
    const std::vector<uint32_t>& GetPhysicalQpIds(void) const {
        return m_physicalQpIds;
    }

    /**
     * Enable usage with out streams
     */
    friend std::ostream &operator<<(std::ostream& os, const IbRemoteInfo& o) {
        std::ostream& ret = os << "NodeId: 0x" << std::hex << o.m_nodeId <<
            ", Lid: 0x" << std::hex << o.m_lid;

        for (auto& it : o.m_physicalQpIds) {
            os << ", Physical QP Id: 0x" << std::hex << it;
        }

        return os;
    }

private:
    uint16_t m_nodeId;
    uint16_t m_lid;
    uint32_t m_conManIdent;
    std::vector<uint32_t> m_physicalQpIds;
};

}
}

#endif // IBNET_CORE_IBREMOTEINFO_H
