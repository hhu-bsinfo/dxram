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

#include "ConnectionCreator.h"

namespace ibnet {
namespace dx {

ConnectionCreator::ConnectionCreator(uint16_t qpMaxSendReqs,
        uint16_t qpMaxRecvReqs, uint16_t qpFlowControlMaxRecvReqs,
        std::shared_ptr<core::IbSharedRecvQueue> sharedRecvQueue,
        std::shared_ptr<core::IbCompQueue> sharedRecvCompQueue,
        std::shared_ptr<core::IbSharedRecvQueue> sharedFlowControlRecvQueue,
        std::shared_ptr<core::IbCompQueue> sharedFlowControlRecvCompQueue) :
    m_qpMaxSendReqs(qpMaxSendReqs),
    m_qpMaxRecvReqs(qpMaxRecvReqs),
    m_qpFlowControlMaxRecvReqs(qpFlowControlMaxRecvReqs),
    m_sharedRecvQueue(sharedRecvQueue),
    m_sharedRecvCompQueue(sharedRecvCompQueue),
    m_sharedFlowControlRecvQueue(sharedFlowControlRecvQueue),
    m_sharedFlowControlRecvCompQueue(sharedFlowControlRecvCompQueue)
{

}

ConnectionCreator::~ConnectionCreator(void)
{

}

std::shared_ptr<core::IbConnection> ConnectionCreator::CreateConnection(
    uint16_t connectionId,
    std::shared_ptr<core::IbDevice>& device,
    std::shared_ptr<core::IbProtDom>& protDom)
{
    auto ret = std::make_shared<core::IbConnection>(connectionId, device,
        protDom);

    ret->AddQp(m_sharedRecvQueue, m_sharedRecvCompQueue, m_qpMaxRecvReqs,
        m_qpMaxSendReqs);
    // a single work request is enough because we sum up flow control data
    ret->AddQp(m_sharedFlowControlRecvQueue, m_sharedFlowControlRecvCompQueue,
        m_qpFlowControlMaxRecvReqs, 1);

    return ret;
}

}
}