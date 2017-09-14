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

#ifndef IBNET_CORE_IBCONNECTIONCREATOR_H
#define IBNET_CORE_IBCONNECTIONCREATOR_H

#include "IbDevice.h"
#include "IbConnection.h"
#include "IbProtDom.h"

namespace ibnet {
namespace core {

/**
 * Interface for a connection creator telling the connection manager how
 * to setup a newly established connection.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class IbConnectionCreator
{
public:
    /**
     * Constructor
     */
    IbConnectionCreator(void) {};

    /**
     * Destructor
     */
    virtual ~IbConnectionCreator(void) {};

    /**
     * Create a new connection
     *
     * @param connectionId Id for the new connection
     * @param device Device
     * @param protDom Protection domain
     * @return Instance of a new connection
     */
    virtual std::shared_ptr<IbConnection> CreateConnection(
        uint16_t connectionId,
        std::shared_ptr<IbDevice>& device,
        std::shared_ptr<IbProtDom>& protDom) = 0;
};

}
}

#endif //IBNET_CORE_IBCONNECTIONCREATOR_H
