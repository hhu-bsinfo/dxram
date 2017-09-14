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

#ifndef IBNET_CORE_IBNODECONFREADER_H
#define IBNET_CORE_IBNODECONFREADER_H

#include "IbNodeConf.h"

namespace ibnet {
namespace core {

/**
 * Interface for a node configuration reader to get a list of nodes forming
 * an InfiniBand network
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class IbNodeConfReader
{
public:
    /**
     * Constructor
     */
    IbNodeConfReader(void) {};

    /**
     * Desturctor
     */
    virtual ~IbNodeConfReader(void) {};

    /**
     * Read the configuration
     *
     * @return IbNodeConf object with nodes of the network
     */
    virtual IbNodeConf Read(void) = 0;
};

}
}

#endif // IBNET_CORE_IBNODECONFREADER_H
