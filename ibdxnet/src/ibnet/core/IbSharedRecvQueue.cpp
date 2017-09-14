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

#include "IbSharedRecvQueue.h"

#include "ibnet/sys/Logger.hpp"

namespace ibnet {
namespace core {

IbSharedRecvQueue::IbSharedRecvQueue(std::shared_ptr<IbProtDom>& protDom,
        uint32_t size) :
    m_size(size),
    m_srq(nullptr)
{
    struct ibv_srq_init_attr attr;

    memset(&attr, 0, sizeof(attr));

    attr.attr.max_sge = 1;
    attr.attr.max_wr = size;

    IBNET_LOG_TRACE("ibv_create_srq, size {}", size);
    m_srq = ibv_create_srq(protDom->GetIBProtDom(), &attr);

    if (m_srq == nullptr) {
        IBNET_LOG_ERROR("Creating shared receive queue failed: {}",
            strerror(errno));
        throw IbException("Creating shared receive queue failed");
    }
}

IbSharedRecvQueue::~IbSharedRecvQueue(void)
{
    ibv_destroy_srq(m_srq);
}

}
}