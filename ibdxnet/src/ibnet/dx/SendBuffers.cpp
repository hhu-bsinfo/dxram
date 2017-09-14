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

#include "SendBuffers.h"

#include "ibnet/sys/Logger.hpp"

namespace ibnet {
namespace dx {

SendBuffers::SendBuffers(uint32_t bufferSize, uint32_t maxConnections,
        std::shared_ptr<core::IbProtDom>& protDom)
{
    IBNET_LOG_INFO(
        "Allocating send buffer pool for {} connections, buffer size {}",
        maxConnections, bufferSize);

    for (uint32_t i = 0; i < maxConnections; i++) {
        m_buffers.push_back(protDom->Register(malloc(bufferSize),
            bufferSize, true));
    }

    for (uint32_t i = 0; i < maxConnections; i++) {
        m_flowControlBuffers.push_back(protDom->Register(
            malloc(sizeof(uint32_t)), sizeof(uint32_t), true));
    }
}

SendBuffers::~SendBuffers(void)
{

}

}
}