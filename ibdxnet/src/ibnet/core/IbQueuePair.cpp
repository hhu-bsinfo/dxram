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

#include "IbQueuePair.h"

#include "ibnet/sys/Logger.hpp"

#define DEFAULT_IB_PORT 1

namespace ibnet {
namespace core {

IbQueuePair::IbQueuePair(
        std::shared_ptr<IbDevice>& device,
        std::shared_ptr<IbProtDom>& protDom,
        uint16_t maxSendReqs,
        uint16_t maxRecvReqs,
        std::shared_ptr<IbCompQueue>& sharedRecvCompQueue,
        std::shared_ptr<IbSharedRecvQueue>& sharedRecvQueue) :
    m_sendQueue(std::make_unique<IbSendQueue>(device, *this, maxSendReqs)),
    m_recvQueue(std::make_unique<IbRecvQueue>(device, *this, maxRecvReqs,
        sharedRecvCompQueue, sharedRecvQueue)),
    m_ibQp(nullptr),
    m_qpNum((uint32_t) -1)
{
    IBNET_LOG_TRACE_FUNC;

    try {
        __CreateQP(protDom);
        __SetInitState();
    } catch (const IbException& e) {
        __Cleanup();
        throw e;
    }

    IBNET_LOG_TRACE("Created QP, qpNum {:x}", m_qpNum);
}

IbQueuePair::~IbQueuePair(void)
{
    __Cleanup();
}

void IbQueuePair::Close(bool force)
{
    m_sendQueue->Close(force);
    m_recvQueue->Close(force);
}

void IbQueuePair::__CreateQP(std::shared_ptr<IbProtDom>& protDom)
{
    IBNET_LOG_TRACE_FUNC;

    ibv_qp_init_attr qp_init_attr;

    memset(&qp_init_attr, 0, sizeof(ibv_qp_init_attr));

    qp_init_attr.send_cq = m_sendQueue->m_compQueue->GetCQ();
    qp_init_attr.recv_cq = m_recvQueue->m_compQueue->GetCQ();
    // reliable connection
    qp_init_attr.qp_type = IBV_QPT_RC;

    // if available, use a srq
    if (m_recvQueue->IsRecvQueueShared()) {
        IBNET_LOG_DEBUG("Using shared recv work queue");
        qp_init_attr.srq = m_recvQueue->m_sharedRecvQueue->GetQueue();
    }

    qp_init_attr.cap.max_send_wr = m_sendQueue->GetQueueSize();
    qp_init_attr.cap.max_recv_wr = m_recvQueue->GetQueueSize();
    // S/G = Scatter / Gather
    // #S/G entries per snd req
    qp_init_attr.cap.max_send_sge = 1;
    // #S/G entries per rcv req
    qp_init_attr.cap.max_recv_sge = 1;
    // req inline data in bytes
    qp_init_attr.cap.max_inline_data = 0;
    // only generate CQ elements on requested WQ elements
    qp_init_attr.sq_sig_all = 0;

    IBNET_LOG_TRACE("ibv_create_qp");
    m_ibQp = ibv_create_qp(protDom->GetIBProtDom(), &qp_init_attr);

    if (m_ibQp == nullptr) {
        IBNET_LOG_ERROR("Creating queue pair failed: {}", strerror(errno));
        throw IbException("Creating queue pair failed");
    }

    m_qpNum = m_ibQp->qp_num;
}

void IbQueuePair::__SetInitState(void)
{
    IBNET_LOG_TRACE_FUNC;

    // init queue pair. the queue pair needs to be set to ready to
    // send and receive after this
    int result;
    struct ibv_qp_attr qp_attr;

    memset(&qp_attr, 0, sizeof(struct ibv_qp_attr));

    qp_attr.qp_state        = IBV_QPS_INIT;
    qp_attr.pkey_index      = 0;
    qp_attr.port_num        = DEFAULT_IB_PORT;
    qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE;

    // modify queue pair attributes
    IBNET_LOG_TRACE("ibv_modify_qp");
    result = ibv_modify_qp(
            m_ibQp, &qp_attr,
            IBV_QP_STATE        |
            IBV_QP_PKEY_INDEX   |
            IBV_QP_PORT         |
            IBV_QP_ACCESS_FLAGS);

    if (result != 0) {
        IBNET_LOG_ERROR("Setting queue pair state to init failed: {}",
                strerror(result));
        throw IbException("Setting queue pair state to init failed");
    }
}

void IbQueuePair::__Cleanup(void)
{
    IBNET_LOG_TRACE_FUNC;

    m_sendQueue.reset();
    m_recvQueue.reset();

    if (m_ibQp != nullptr) {
        IBNET_LOG_TRACE("ibv_destroy_qp");
        ibv_destroy_qp(m_ibQp);
        m_ibQp = nullptr;
    }
}

}
}