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

#include "IbConnection.h"

#include "ibnet/sys/Logger.hpp"

namespace ibnet {
namespace core {

IbConnection::IbConnection(
        uint16_t connectionId,
        std::shared_ptr<IbDevice>& device,
        std::shared_ptr<IbProtDom>& protDom) :
    m_connectionId(connectionId),
    m_device(device),
    m_protDom(protDom),
    m_isConnected(false)
{

}

IbConnection::~IbConnection(void)
{
    for (auto& it : m_qps) {
        it.reset();
    }

    m_qps.clear();
}

void IbConnection::AddQp(std::shared_ptr<IbSharedRecvQueue>& sharedRecvQueue,
       std::shared_ptr<IbCompQueue>& sharedRecvCompQueue,
       uint16_t maxRecvReqs, uint16_t maxSendReqs)
{
    m_qps.push_back(std::make_shared<IbQueuePair>(m_device, m_protDom,
        maxSendReqs, maxRecvReqs, sharedRecvCompQueue, sharedRecvQueue));
}

void IbConnection::Connect(const IbRemoteInfo& remoteInfo)
{
    IBNET_LOG_TRACE_FUNC;

    m_remoteInfo = remoteInfo;

    if (remoteInfo.GetPhysicalQpIds().size() != m_qps.size()) {
        throw IbException("Number of queue pairs " +
            std::to_string(remoteInfo.GetPhysicalQpIds().size()) +
            " does not match number of remote physical QP ids (" +
            std::to_string(m_qps.size()) + ")");
    }

    for (size_t i = 0; i < m_qps.size(); i++) {
        m_qps[i]->GetRecvQueue()->Open(m_remoteInfo.GetLid(),
            m_remoteInfo.GetPhysicalQpIds()[i]);
        m_qps[i]->GetSendQueue()->Open();
    }

    m_isConnected.store(true, std::memory_order_relaxed);
}

void IbConnection::Close(bool force)
{
    IBNET_LOG_TRACE_FUNC;

    // flush outstanding sends on qps
    if (!force) {
        for (auto& it : m_qps) {
            it->GetSendQueue()->Flush();
        }
    }

    for (auto& it : m_qps) {
        it->Close(force);
        it.reset();
    }

    m_qps.clear();

    m_isConnected.store(false, std::memory_order_relaxed);
}

}
}