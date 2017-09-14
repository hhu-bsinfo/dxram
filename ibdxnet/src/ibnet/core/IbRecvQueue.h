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

#ifndef IBNET_CORE_IBRECVQUEUE_H
#define IBNET_CORE_IBRECVQUEUE_H

#include <cstdint>

#include "IbCompQueue.h"
#include "IbMemReg.h"
#include "IbQueueClosedException.h"
#include "IbQueueTracker.h"
#include "IbSharedRecvQueue.h"

namespace ibnet {
namespace core {

// forward declaration
class IbQueuePair;

/**
 * Recv queue, part of an IbQueueQPair
 *
 * @see IbQueuePair
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class IbRecvQueue
{
public:
    friend class IbQueuePair;

    /**
     * Constructor
     *
     * @param device Pointer to a device to create the queue for
     * @param parentQp Parent queue pair of this recv queue
     * @param queueSize Size of the recv queue
     * @param sharedCompQueue Optional shared completion queue. If none
     *          provided, a dedicated completion queue is created.
     * @param sharedRecvQueue Optional shared recv queue. If none provided,
     *          a dedicated recv queue is created.
     */
    IbRecvQueue(std::shared_ptr<IbDevice>& device, IbQueuePair& parentQp,
                uint16_t queueSize,
                std::shared_ptr<IbCompQueue> sharedCompQueue = nullptr,
                std::shared_ptr<IbSharedRecvQueue> sharedRecvQueue = nullptr);

    /**
     * Destructor
     */
    ~IbRecvQueue();

    /**
     * Get the size of the queue
     */
    uint32_t GetQueueSize(void) const {
        return m_queueSize;
    }

    /**
     * Check if the recv queue is shared
     * @return True if shared, false on dedicated recv queue
     */
    bool IsRecvQueueShared(void) const {
        return m_recvQueueIsShared;
    }

    /**
     * Check if the completion queue is shared
     * @return True if shared, false on dedicated completion queue
     */
    bool IsCompQueueShared(void) const {
        return m_compQueueIsShared;
    }

    /**
     * Open the queue, i.e. set it ready to receive.
     *
     * Ensure to call this before opening the send queue of a queue pair
     *
     * @param remoteQpLid Lid of the remote queue pair to connect to
     * @param remoteQpPhysicalId  Physical QP id of the remote queue pair to
     *                              connect to
     */
    void Open(uint16_t remoteQpLid, uint32_t remoteQpPhysicalId);

    /**
     * Close the queue. When closed, all calls to the queue throw
     * IbQueueClosedExceptions
     *
     * @param force Force close, i.e. don't wait for tasks to finish processing
     */
    void Close(bool force);

    /**
     * Post a message to allow receiving data from a remote send post
     *
     * @param memReg Memory region to write the receiving data to
     * @param workReqId Work request id to assign to the InfiniBand work
     *                  request
     */
    void Receive(const IbMemReg* memReg, uint64_t workReqId = 0);

    /**
     * Poll the next enqueued work request until it completed
     *
     * @param blocking True to busy pull until it completed, false to try
     *          polling once and return even if it hasn't completed
     * @return The queue pair id of the successfully completed work request or
     *          -1 if queue empty and no work request completed (non blocking)
     */
    uint32_t PollCompletion(bool blocking = true);

    /**
     * Blocking poll all remaining work requests until they completed
     *
     * @return Number of remaining work requests completed
     */
    uint32_t Flush(void);

private:
    IbQueuePair& m_parentQp;
    uint16_t m_queueSize;
    bool m_compQueueIsShared;
    bool m_recvQueueIsShared;
    bool m_isClosed;

    std::shared_ptr<IbCompQueue> m_compQueue;
    std::shared_ptr<IbSharedRecvQueue> m_sharedRecvQueue;
};

}
}

#endif //IBNET_CORE_IBRECVQUEUE_H
