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

#ifndef IBNET_CORE_IBCONNECTIONCREATORSIMPLE_H
#define IBNET_CORE_IBCONNECTIONCREATORSIMPLE_H

#include "IbConnectionCreator.h"

namespace ibnet {
namespace core {

/**
 * Implementation of a connection creator which creates a connection with
 * a single queue pair
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class IbConnectionCreatorSimple : public IbConnectionCreator
{
public:
    /**
     * Constructor
     *
     * @param qpMaxRecvReqs Size of the receive queue
     * @param qpMaxSendReqs Size of the send queue
     * @param sharedRecvQueue Shared receive queue to use (optional)
     * @param sharedRecvCompQueue Shared receive completion queue to use
     *          (optional)
     */
    IbConnectionCreatorSimple(uint16_t qpMaxRecvReqs, uint16_t qpMaxSendReqs,
        std::shared_ptr<IbSharedRecvQueue> sharedRecvQueue,
        std::shared_ptr<IbCompQueue> sharedRecvCompQueue);

    /**
     * Destructor
     */
    ~IbConnectionCreatorSimple(void);

    /**
     * Override
     */
    std::shared_ptr<IbConnection> CreateConnection(
        uint16_t connectionId,
        std::shared_ptr<IbDevice>& device,
        std::shared_ptr<IbProtDom>& protDom) override;

private:
    uint16_t m_qpMaxRecvReqs;
    uint16_t m_qpMaxSendReqs;
    std::shared_ptr<IbSharedRecvQueue> m_sharedRecvQueue;
    std::shared_ptr<IbCompQueue> m_sharedRecvCompQueue;
};

}
}

#endif //PROJECT_IBCONNECTIONCREATORSIMPLE_H
