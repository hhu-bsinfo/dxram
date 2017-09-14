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

#ifndef IBNET_CORE_IBSENDQUEUE_H
#define IBNET_CORE_IBSENDQUEUE_H

#include <cstdint>

#include "IbCompQueue.h"
#include "IbException.h"
#include "IbMemReg.h"
#include "IbQueueClosedException.h"

namespace ibnet {
namespace core {

// forward declaration
class IbQueuePair;

/**
 * Send queue, part of an IbQueueQPair
 *
 * @see IbQueuePair
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class IbSendQueue
{
public:
    friend class IbQueuePair;

    /**
     * Constructor
     *
     * @param device Pointer to a device to create the queue for
     * @param parentQp Parent queue pair of this send queue
     * @param queueSize Size of the send queue
     */
    IbSendQueue(std::shared_ptr<IbDevice>& device, IbQueuePair& parentQp,
                uint16_t queueSize);

    /**
     * Destructor
     */
    ~IbSendQueue();

    /**
     * Get the size of the queue
     */
    uint16_t GetQueueSize(void) const {
        return m_queueSize;
    }

    /**
     * Open the queue, i.e. set it ready to send
     */
    void Open(void);

    /**
     * Close the queue. When closed, all calls to the queue throw
     * IbQueueClosedExceptions
     *
     * @param force Force close, i.e. don't wait for tasks to finish processing
     */
    void Close(bool force);

    /**
     * Posts a message send work request to the (parent) queue pair
     *
     * @param memReg Memory region with data to send
     * @param offset Start offset in memory region for data to send
     * @param size Number of bytes to send
     * @param workReqId Work request id to assign to the InfiniBand work request
     */
    void Send(const IbMemReg* memReg, uint32_t offset = 0,
              uint32_t size = (uint32_t) -1, uint64_t workReqId = 0);

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
    bool m_isClosed;

    std::unique_ptr<IbCompQueue> m_compQueue;
};

}
}

#endif //IBNET_CORE_IBSENDQUEUE_H
