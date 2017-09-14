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

#ifndef PROJECT_IBQUEUETRACKER_H
#define PROJECT_IBQUEUETRACKER_H

#include <atomic>
#include <cstdbool>
#include <cstdint>

#include "IbException.h"

namespace ibnet {
namespace core {

/**
 * Keeps track of work requests on queues. The ibverbs API does not provide any
 * mechanism to check how many elements are currently enqueued or have finished
 * processing (I assume getting this information would be ver expensive
 * because it's residing on the HCA). Thus, we have to track submissions of
 * work requests which tells us how many completions we have to poll on the
 * completion queues to not over-/underrun the queues.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class IbQueueTracker
{
public:
    /**
     * Constructor
     *
     * @param size Size of the tracker (equivalent to queue size)
     */
    IbQueueTracker(uint16_t size) :
        m_size(size),
        m_outstanding(0)
    {};

    /**
     * Destructor
     */
    ~IbQueueTracker(void) {};

    /**
     * Increase the counter if a new work request was submitted
     *
     * @return True if increased, false if increasing not possible because
     *          queue is full
     */
    inline bool AddOutstanding(void) {
        uint16_t outstandingSends =
            m_outstanding.load(std::memory_order_relaxed);

        do {
            if (outstandingSends == m_size) {
                return false;
            }
        } while (!m_outstanding.compare_exchange_weak(outstandingSends,
            outstandingSends + 1, std::memory_order_relaxed));

        return true;
    }

    /**
     * Decrease the counter if a work completion was successfully polled for
     * a work request
     *
     * @return True if decreasing successful, false if queue empty
     */
    inline bool SubOutstanding(void) {
        uint16_t tmp = m_outstanding.load(std::memory_order_relaxed);

        do {
            if (tmp == 0) {
                return false;
            }
        } while (!m_outstanding.compare_exchange_weak(tmp, tmp - 1,
            std::memory_order_relaxed));

        return true;
    }

    /**
     * Get the current number of outstanding requests
     */
    inline uint16_t GetCurrent(void) const {
        return m_outstanding.load(std::memory_order_relaxed);
    }

private:
    const uint16_t m_size;
    std::atomic<uint16_t> m_outstanding;
};

}
}

#endif //PROJECT_IBQUEUETRACKER_H
