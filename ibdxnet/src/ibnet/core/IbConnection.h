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

#ifndef IBNET_CORE_IBCONNECTION_H
#define IBNET_CORE_IBCONNECTION_H

#include <infiniband/verbs.h>

#include "IbCompQueue.h"
#include "IbNodeNotAvailableException.h"
#include "IbQueuePair.h"
#include "IbRemoteInfo.h"

namespace ibnet {
namespace core {

/**
 * Instance of a logical connection with a remote node. A connection can
 * consist of one or multiple queue pairs
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class IbConnection
{
public:
    /**
     * Constructor
     *
     * @param connectionId Unique id assigned to the connection
     * @param device Device
     * @param protDom Protection domain
     */
    IbConnection(
        uint16_t connectionId,
        std::shared_ptr<IbDevice>& device,
        std::shared_ptr<IbProtDom>& protDom);

    /**
     * Destructor
     */
    ~IbConnection(void);

    /**
     * Get the connection id
     */
    uint16_t GetConnectionId(void) const {
        return m_connectionId;
    }

    /**
     * Get the node id of the remote node
     */
    uint16_t GetRemoteNodeId(void) const {
        return m_remoteInfo.GetNodeId();
    }

    /**
     * Get the remote node information
     */
    const IbRemoteInfo& GetRemoteInfo(void) const {
        return m_remoteInfo;
    }

    /**
     * Create and add a new queue pair to the connection
     *
     * @param sharedRecvQueue Shared receive queue to use (optional)
     * @param sharedRecvCompQueue Shared receive completion queue to use
     *          (optional)
     * @param maxRecvReqs Receive queue size
     * @param maxSendReqs Send queue size
     */
    void AddQp(std::shared_ptr<IbSharedRecvQueue>& sharedRecvQueue,
        std::shared_ptr<IbCompQueue>& sharedRecvCompQueue,
        uint16_t maxRecvReqs,
        uint16_t maxSendReqs);

    /**
     * Get a queue pair
     *
     * Do not keep/store the shared pointer. If you want to operate on a
     * queue pair, always call this function to get it.
     *
     * @param idx Queue pair id/index
     * @return Pointer to the queue pair
     */
    std::shared_ptr<IbQueuePair>& GetQp(uint32_t idx) {
        return m_qps.at(idx);
    }

    /**
     * Get all queue pairs
     */
    const std::vector<std::shared_ptr<IbQueuePair>>& GetQps(void) const {
        return m_qps;
    }

    /**
     * Check if the connection is up
     */
    bool IsConnected(void) const {
        return m_isConnected.load(std::memory_order_relaxed);
    }

    /**
     * Connect to a remote node
     *
     * This needs to be called before any operation on queue pairs is possible
     *
     * @param remoteInfo Remote info of node to connect to
     */
    void Connect(const IbRemoteInfo& remoteInfo);

    /**
     * Close the established connection
     *
     * @param force True to force close, i.e. don't wait until all work
     *          requests of all queue pairs are processed, false to wait
     *          for oustanding work requests to be completed
     */
    void Close(bool force);

    /**
     * Enable output to an out stream
     */
    friend std::ostream &operator<<(std::ostream& os, const IbConnection& o) {
        return os << "Connected: " << o.m_isConnected <<
                     ", RemoteInfo: " << o.m_remoteInfo;
    }

private:
    uint16_t m_connectionId;
    std::shared_ptr<IbDevice> m_device;
    std::shared_ptr<IbProtDom> m_protDom;

    IbRemoteInfo m_remoteInfo;
    std::vector<std::shared_ptr<IbQueuePair>> m_qps;
    std::atomic<bool> m_isConnected;
};

}
}

#endif // IBNET_CORE_IBCONNECTION_H
