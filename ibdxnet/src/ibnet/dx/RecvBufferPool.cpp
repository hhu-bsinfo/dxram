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

#include "RecvBufferPool.h"

#include "ibnet/sys/Logger.hpp"
#include "DxnetException.h"

namespace ibnet {
namespace dx {

RecvBufferPool::RecvBufferPool(uint64_t initialTotalPoolSize,
        uint32_t recvBufferSize, uint32_t flowControlQueueSize,
        std::shared_ptr<core::IbProtDom>& protDom) :
    m_bufferPoolSize(initialTotalPoolSize / recvBufferSize),
    m_bufferSize(recvBufferSize),
    m_numFlowControlBuffers(flowControlQueueSize),
    m_dataBuffersFront(0),
    m_dataBuffersBack(
        (uint32_t) (initialTotalPoolSize / recvBufferSize - 1)),
    m_dataBuffersBackRes(
        (uint32_t) (initialTotalPoolSize / recvBufferSize - 1)),
    m_protDom(protDom)
{
    IBNET_LOG_INFO("Alloc {} data buffers, size {} each",
        m_bufferPoolSize, recvBufferSize);

    // to handle wrap around correctly
    if (m_bufferPoolSize % 2 != 0) {
        throw DxnetException("RecvBufferPool: Resulting pool size must be a "
            "power of two, invalid value: " + m_bufferPoolSize);
    }

    m_dataBuffers = new core::IbMemReg*[m_bufferPoolSize];
    for (uint32_t i = 0; i < m_bufferPoolSize; i++) {
        m_dataBuffers[i] = m_protDom->Register(
            malloc(recvBufferSize), recvBufferSize, true);
    }

    IBNET_LOG_INFO("Alloc {} fc buffers", m_numFlowControlBuffers);

    for (uint32_t i = 0; i < m_numFlowControlBuffers; i++) {
        m_flowControlBuffers.push_back(m_protDom->Register(
            malloc(4), 4, true));
    }
}

RecvBufferPool::~RecvBufferPool(void)
{

}

core::IbMemReg* RecvBufferPool::GetBuffer(void)
{
    core::IbMemReg* buffer = NULL;

    bool warnOnce = true;
    uint32_t front = m_dataBuffersFront.load(std::memory_order_relaxed);
    uint32_t back;

    while (true) {
        back = m_dataBuffersBack.load(std::memory_order_relaxed);

        if (front % m_bufferPoolSize == back % m_bufferPoolSize) {
            if (warnOnce) {
                warnOnce = false;
                IBNET_LOG_WARN("Insufficient pooled incoming buffers... "
                    "waiting for buffers to get returned. If this warning "
                    "appears periodically and very frequently, consider "
                    "increasing the receive pool's total size to avoid "
                    "possible performance penalties");
            }

            continue;
        }

        buffer = m_dataBuffers[front % m_bufferPoolSize];

        m_dataBuffersFront.fetch_add(1, std::memory_order_release);
        break;
    }

    return buffer;
}

void RecvBufferPool::ReturnBuffer(core::IbMemReg* buffer)
{
    uint32_t backRes = m_dataBuffersBackRes.load(std::memory_order_relaxed);
    uint32_t front;

    while (true) {
        front = m_dataBuffersFront.load(std::memory_order_relaxed);

        if (backRes + 1 % m_bufferPoolSize == front % m_bufferPoolSize) {
            IBNET_LOG_PANIC("Pool overflow, this should not happen");
            break;
        }

        if (m_dataBuffersBackRes.compare_exchange_weak(backRes, backRes + 1,
                std::memory_order_relaxed)) {
            m_dataBuffers[backRes % m_bufferPoolSize] = buffer;

            // if two buffers are returned at the same time, the first return
            // could be interrupt by a second return. the reserve of the first
            // return is already completed but the back pointer is not updated.
            // the second return reserves and updates the back pointer. now,
            // the back pointer is pointing to the first returns reserve which
            // might not be completed, yet.
            // solution: the second return has to wait for the first return
            // to complete, both, the reservation and updating of the back
            // pointer before it can update the back pointer as well
            while (!m_dataBuffersBack.compare_exchange_weak(backRes,
                    backRes + 1, std::memory_order_release)) {
                std::this_thread::yield();
            }

            break;
        }
    }
}

core::IbMemReg* RecvBufferPool::GetFlowControlBuffer(void)
{
    core::IbMemReg* buffer = NULL;

    if (!m_flowControlBuffers.empty()) {
        buffer = m_flowControlBuffers.back();
        m_flowControlBuffers.pop_back();
    } else {
        IBNET_LOG_ERROR("Out of flow control buffers");
    }

    return buffer;
}

}
}