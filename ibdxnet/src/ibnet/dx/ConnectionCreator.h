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

#ifndef IBNET_DX_CONNECTIONCREATOR_H
#define IBNET_DX_CONNECTIONCREATOR_H

#include "ibnet/core/IbCompQueue.h"
#include "ibnet/core/IbConnectionCreator.h"
#include "ibnet/core/IbSharedRecvQueue.h"

namespace ibnet {
namespace dx {

/**
 * Connection creator for the dxnet subsystem. Creates two queue pairs
 * for each connection, one QP for data buffers, one for flow
 * control data.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 02.06.2017
 */
class ConnectionCreator : public ibnet::core::IbConnectionCreator
{
public:
    /**
     * Constructor
     *
     * @param qpMaxSendReqs Size of the buffer send queue
     * @param qpMaxRecvReqs Size of the buffer receive queue
     * @param qpFlowControlMaxRecvReqs Size of the flow control receive queue
     * @param sharedRecvQueue Shared receive queue for buffers
     * @param sharedRecvCompQueue Shared completion queue for buffers
     * @param sharedFlowControlRecvQueue Shared receive queue for FC data
     * @param sharedFlowControlRecvCompQueue Shared completion queue for FC data
     */
    ConnectionCreator(uint16_t qpMaxSendReqs, uint16_t qpMaxRecvReqs,
        uint16_t qpFlowControlMaxRecvReqs,
        std::shared_ptr<core::IbSharedRecvQueue> sharedRecvQueue,
        std::shared_ptr<core::IbCompQueue> sharedRecvCompQueue,
        std::shared_ptr<core::IbSharedRecvQueue> sharedFlowControlRecvQueue,
        std::shared_ptr<core::IbCompQueue> sharedFlowControlRecvCompQueue);

    /**
     * Destructor
     */
    ~ConnectionCreator(void);

    /**
     * Override
     */
    std::shared_ptr<core::IbConnection> CreateConnection(
        uint16_t connectionId,
        std::shared_ptr<core::IbDevice>& device,
        std::shared_ptr<core::IbProtDom>& protDom) override;

private:
    uint16_t m_qpMaxSendReqs;
    uint16_t m_qpMaxRecvReqs;
    uint16_t m_qpFlowControlMaxRecvReqs;
    std::shared_ptr<core::IbSharedRecvQueue> m_sharedRecvQueue;
    std::shared_ptr<core::IbCompQueue> m_sharedRecvCompQueue;
    std::shared_ptr<core::IbSharedRecvQueue> m_sharedFlowControlRecvQueue;
    std::shared_ptr<core::IbCompQueue> m_sharedFlowControlRecvCompQueue;
};

}
}

#endif //IBNET_DX_CONNECTIONCREATOR_H
