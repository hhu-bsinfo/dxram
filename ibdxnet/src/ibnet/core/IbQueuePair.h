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

#ifndef IBNET_CORE_IBQUEUEPAIR_H
#define IBNET_CORE_IBQUEUEPAIR_H

#include <infiniband/verbs.h>

#include "IbDevice.h"
#include "IbProtDom.h"
#include "IbRemoteInfo.h"
#include "IbRecvQueue.h"
#include "IbSendQueue.h"

namespace ibnet {
namespace core {

/**
 * Class to wrap a send and receive queue to a queue pair
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class IbQueuePair
{
public:
    /**
     * Constructor
     *
     * This creates a new separate send and receive queue
     *
     * @param device Pointer to a device to create the queue pair for
     * @param protDom Pointer to a protection domain to create the queue pair in
     * @param maxSendReqs Max num of send reqs for the send queue
     *                      (i.e. size of the send queue)
     * @param maxRecvReqs Max num of recv requs for the recv queue
     *                      (i.e. size of the recv queue)
     * @param sharedRecvCompQueue Optional shared recv completion queue
     * @param sharedRecvQueue Optional shared recv queue
     */
    IbQueuePair(
        std::shared_ptr<IbDevice>& device,
        std::shared_ptr<IbProtDom>& protDom,
        uint16_t maxSendReqs,
        uint16_t maxRecvReqs,
        std::shared_ptr<IbCompQueue>& sharedRecvCompQueue,
        std::shared_ptr<IbSharedRecvQueue>& sharedRecvQueue);

    /**
     * Destructor
     */
    ~IbQueuePair(void);

    /**
     * Close the queue pair
     *
     * @param force Force close, i.e. don't wait for tasks to finish processing
     */
    void Close(bool force);

    /**
     * Get the pyhsical queue pair number
     */
    uint32_t GetPhysicalQpNum(void) const {
        return m_qpNum;
    }

    /**
     * Get the IB queue pair object. Used by other parts of the package but
     * no need for the "user"
     */
    ibv_qp* GetIbQp(void) const {
        return m_ibQp;
    }

    /**
     * Get the send queue
     */
    std::unique_ptr<IbSendQueue>& GetSendQueue(void) {
        return m_sendQueue;
    }

    /**
     * Get the receive queue
     */
    std::unique_ptr<IbRecvQueue>& GetRecvQueue(void) {
        return m_recvQueue;
    }

private:
    std::unique_ptr<IbSendQueue> m_sendQueue;
    std::unique_ptr<IbRecvQueue> m_recvQueue;

    ibv_qp* m_ibQp;
    uint32_t m_qpNum;

    void __CreateQP(std::shared_ptr<IbProtDom>& protDom);
    void __SetInitState(void);

    void __Cleanup(void);
};

}
}

#endif // IBNET_CORE_IBQUEUEPAIR_H
