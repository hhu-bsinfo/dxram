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

#include "IbRecvQueue.h"

#include "ibnet/sys/Logger.hpp"

#include "IbQueueFullException.h"
#include "IbQueuePair.h"

#define DEFAULT_IB_PORT 1
#define IB_QOS_LEVEL 1

namespace ibnet {
namespace core {

IbRecvQueue::IbRecvQueue(std::shared_ptr<IbDevice>& device,
        IbQueuePair& parentQp, uint16_t queueSize,
        std::shared_ptr<IbCompQueue> sharedCompQueue,
        std::shared_ptr<IbSharedRecvQueue> sharedRecvQueue) :
    m_parentQp(parentQp),
    m_queueSize(queueSize),
    m_compQueueIsShared(true),
    m_recvQueueIsShared(true),
    m_isClosed(false),
    m_compQueue(sharedCompQueue),
    m_sharedRecvQueue(sharedRecvQueue)
{
    if (m_sharedRecvQueue == nullptr) {
        m_recvQueueIsShared = false;
    } else {
        IBNET_LOG_DEBUG("Using shared recv queue");
    }

    // create non shared/private comp queue, match size of max work requests
    if (m_compQueue == nullptr) {
        m_compQueue = std::make_shared<IbCompQueue>(device, queueSize);
        m_compQueueIsShared = false;
    } else {
        IBNET_LOG_DEBUG("Using shared recv completion queue");
    }
}

IbRecvQueue::~IbRecvQueue()
{
    // private queue is deleted here
    m_compQueue.reset();
}

void IbRecvQueue::Open(uint16_t remoteQpLid, uint32_t remoteQpPhysicalId)
{
    IBNET_LOG_TRACE_FUNC;

    struct ibv_qp_attr attr;
    int result = 0;

    // change state to ready to receive
    memset(&attr, 0, sizeof(struct ibv_qp_attr));

    // ready to receive state
    attr.qp_state = IBV_QPS_RTR;
    // MTU SIZE = 2048 bytes
    attr.path_mtu = IBV_MTU_2048;
    // server qp_num
    attr.dest_qp_num = remoteQpPhysicalId;
    // server packet seq. nr.
    attr.rq_psn = 0;
    attr.sq_psn = 0;

    // num of responder resources for
    // incoming RDMA reads & atomic ops
    attr.max_dest_rd_atomic = 1;
    // minimum RNR NAK timer
    attr.min_rnr_timer = 12;
    // global routing header not used
    attr.ah_attr.is_global = 0;
    // LID of remote IB port
    attr.ah_attr.dlid = remoteQpLid;
    // QoS priority
    attr.ah_attr.sl = IB_QOS_LEVEL;
    // default port (for multiport NICs)
    attr.ah_attr.src_path_bits = 0;
    // IB port
    attr.ah_attr.port_num = DEFAULT_IB_PORT;

    // do the state change on the qp
    IBNET_LOG_TRACE("ibv_modify_qp");
    result = ibv_modify_qp(m_parentQp.GetIbQp(), &attr,
        IBV_QP_STATE                |
        IBV_QP_AV                   |
        IBV_QP_PATH_MTU             |
        IBV_QP_DEST_QPN             |
        IBV_QP_RQ_PSN               |
        IBV_QP_MAX_DEST_RD_ATOMIC   |
        IBV_QP_MIN_RNR_TIMER);

    if (result != 0) {
        IBNET_LOG_ERROR(
            "Setting queue pair to ready to receive to connection failed");
        throw IbException("Setting queue pair to ready to receive failed");
    }
}

void IbRecvQueue::Close(bool force)
{
    if (!force) {
        // wait until outstanding completions are finished
        while (m_compQueue->GetCurrentOutstandingCompletions() > 0) {
            std::this_thread::yield();
        }
    }

    m_isClosed = true;
}

void IbRecvQueue::Receive(const IbMemReg* memReg, uint64_t workReqId)
{
    struct ibv_sge sge_list;
    struct ibv_recv_wr wr;
    // first failed work request
    struct ibv_recv_wr *bad_wr;
    int ret;

    if (m_isClosed) {
        throw IbQueueClosedException();
    }

    // hook memory to write the received data to
    sge_list.addr      		= (uintptr_t) memReg->GetAddress();
    sge_list.length    		= (uint32_t) memReg->GetSize();
    sge_list.lkey      		= memReg->GetLKey();

    // work request for receive operation
    wr.wr_id       			= workReqId;
    wr.sg_list     			= &sge_list;
    wr.num_sge     			= 1;
    wr.next        			= NULL;
    // no opcode or send flags

    // use the srq if available
    if (m_sharedRecvQueue) {
        ret = ibv_post_srq_recv(m_sharedRecvQueue->GetQueue(), &wr, &bad_wr);
    } else {
        ret = ibv_post_recv(m_parentQp.GetIbQp(), &wr, &bad_wr);
    }

    if (ret != 0) {
        switch (ret) {
            case ENOMEM:
                throw IbQueueFullException("Receive queue full");
            default:
                throw IbException(
                    "Posting work request to receive to queue failed (" +
                    std::to_string(ret) + ", mem: " + memReg->ToString());
        }
    }

    m_compQueue->AddOutstandingCompletion();
}

uint32_t IbRecvQueue::PollCompletion(bool blocking)
{
    if (m_isClosed) {
        throw IbQueueClosedException();
    }

    return m_compQueue->PollForCompletion(blocking);
}

uint32_t IbRecvQueue::Flush(void)
{
    if (m_isClosed) {
        throw IbQueueClosedException();
    }

    return m_compQueue->Flush();
}

}
}