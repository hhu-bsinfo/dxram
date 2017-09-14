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

#include "IbProtDom.h"

#include "ibnet/sys/Logger.hpp"

namespace ibnet {
namespace core {

IbProtDom::IbProtDom(std::shared_ptr<IbDevice>& device,
        const std::string& name) :
    m_name(name),
    m_ibProtDom(nullptr)
{
    IBNET_LOG_TRACE_FUNC;
    IBNET_LOG_INFO("[{}] Allocating protection domain", m_name);

    /* allocate protection domain */
    IBNET_LOG_TRACE("ibv_alloc_pd");
    m_ibProtDom = ibv_alloc_pd(device->GetIBContext());

    if (m_ibProtDom == nullptr) {
        IBNET_LOG_ERROR("[{}] Allocating protection domain failed", m_name);
        throw IbException("[{}] Allocating protection domain failed");
    }

    IBNET_LOG_DEBUG("[{}] Allocated protection domain", m_name);
}

IbProtDom::~IbProtDom(void)
{
    IBNET_LOG_TRACE_FUNC;
    IBNET_LOG_DEBUG("[{}] Destroying protection domain", m_name);

    IBNET_LOG_TRACE("[{}] ibv_dereg_mr", m_name);
    for (auto& it : m_registeredRegions) {
        ibv_dereg_mr(it->m_ibMemReg);
    }

    m_registeredRegions.clear();

    IBNET_LOG_TRACE("[{}] ibv_dealloc_pd", m_name);
    ibv_dealloc_pd(m_ibProtDom);

    IBNET_LOG_DEBUG("[{}] Destroying protection domain done", m_name);
}

IbMemReg* IbProtDom::Register(void* addr, uint32_t size, bool freeOnCleanup)
{
    IBNET_LOG_TRACE_FUNC;
    IBNET_ASSERT_PTR(addr);
    IBNET_ASSERT(size != 0);

    m_mutex.lock();

    IBNET_LOG_TRACE("[{}] Registering memory region {:p}, size {}",
            m_name, addr, size);

    IBNET_LOG_TRACE("[{}] ibv_reg_mr", m_name);
    IbMemReg* memReg = new IbMemReg(addr, size, freeOnCleanup);

    memReg->m_ibMemReg = ibv_reg_mr(
            m_ibProtDom,
            addr,
            size,
            IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);

    if (memReg->m_ibMemReg == nullptr) {
        IBNET_LOG_ERROR("[{}] Registering memory region failed: {}",
                m_name, strerror(errno));
        m_mutex.unlock();
        throw IbException("Registering memory region failed");
    }

    m_registeredRegions.push_back(memReg);

    IBNET_LOG_TRACE("[{}] Registering memory region successful, {}",
            m_name, *memReg);

    IbMemReg* ret = m_registeredRegions.back();

    m_mutex.unlock();

    return ret;
}

}
}