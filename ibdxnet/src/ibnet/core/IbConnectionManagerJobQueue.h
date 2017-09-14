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

#ifndef IBNET_CORE_IBCONNECTIONMANAGERJOBQUEUE_H
#define IBNET_CORE_IBCONNECTIONMANAGERJOBQUEUE_H

#include <atomic>
#include <cstdint>
#include <cstring>

#include "IbNodeId.h"

namespace ibnet {
namespace core {

class IbConnectionManagerJobQueue
{
private:
    static const uint32_t MAX_QPS_PER_CONNECTION = 32;

public:
    enum JobType
    {
        JT_INVALID = -1,
        JT_CREATE = 0,
        JT_CLOSE = 1,
        JT_DISCOVERED = 2,
    };

    struct Job
    {
        JobType m_type;
        uint16_t m_nodeId;
        uint32_t m_ident;
        uint32_t m_ipAddr;
        uint16_t m_lid;
        uint32_t m_physicalQpId[MAX_QPS_PER_CONNECTION];
        bool m_force;
        bool m_shutdown;

        Job(void) :
            m_type(JT_INVALID),
            m_nodeId(IbNodeId::INVALID),
            m_ident(0xFFFFFFFF),
            m_ipAddr(0xFFFFFFFF),
            m_lid(0xFFFF),
            m_force(false),
            m_shutdown(false)
        {
            memset(m_physicalQpId, 0xFFFFFFFF,
                sizeof(uint32_t) * MAX_QPS_PER_CONNECTION);
        }
    };

public:
    IbConnectionManagerJobQueue(uint32_t size);
    ~IbConnectionManagerJobQueue(void);

    bool PushBack(const Job& job);

    bool PopFront(Job& job);

    bool IsEmpty(void) const;

private:
    const uint32_t m_size;

    std::atomic<uint32_t> m_front;
    std::atomic<uint32_t> m_back;
    std::atomic<uint32_t> m_backRes;

    Job* m_queue;
};

}
}

#endif //IBNET_CORE_IBCONNECTIONMANAGERJOBQUEUE_H
