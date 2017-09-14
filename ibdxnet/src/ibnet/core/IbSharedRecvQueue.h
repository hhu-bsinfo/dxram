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

#ifndef IBNET_CORE_IBSHAREDRECVQUEUE_H
#define IBNET_CORE_IBSHAREDRECVQUEUE_H

#include <memory>

#include "IbProtDom.h"

namespace ibnet {
namespace core {

/**
 * A shared receive queue can be used on multiple queue pairs.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 01.06.2017
 */
class IbSharedRecvQueue
{
public:
    /**
     * Constructor
     *
     * @param protDom Pointer to a protection domain to create the queue in
     * @param size Size of the queue
     */
    IbSharedRecvQueue(std::shared_ptr<IbProtDom>& protDom, uint32_t size);

    /**
     * Destructor
     */
    ~IbSharedRecvQueue(void);

    /**
     * Get the size of the queue
     */
    uint32_t GetSize(void) const {
        return m_size;
    }

    /**
     * Get the IB queue object. Used by other parts of the package but
     * no need for the "user"
     */
    ibv_srq* GetQueue(void) const {
        return m_srq;
    }

private:
    uint32_t m_size;
    ibv_srq* m_srq;
};

}
}

#endif //IBNET_CORE_IBSHAREDRECVQUEUE_H
