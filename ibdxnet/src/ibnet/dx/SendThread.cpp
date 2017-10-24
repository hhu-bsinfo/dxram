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

#include "SendThread.h"

#include "ibnet/core/IbDisconnectedException.h"

namespace ibnet {
namespace dx {

SendThread::SendThread(uint32_t recvBufferSize,
        std::shared_ptr<SendBuffers> buffers,
        std::shared_ptr<SendHandler>& sendHandler,
        std::shared_ptr<core::IbConnectionManager>& connectionManager) :
    ThreadLoop("SendThread"),
    m_recvBufferSize(recvBufferSize),
    m_buffers(buffers),
    m_sendHandler(sendHandler),
    m_connectionManager(connectionManager),
    m_prevNodeIdWritten(core::IbNodeId::INVALID),
    m_prevDataWritten(0),
    m_ibSendQueueBatchCount(0),
    m_ibSendQueueFullUtilizationCount(0),
    m_sentBytes(0),
    m_sentFlowControlBytes(0),
    m_waitTimer()
{
    m_timers.push_back(sys::ProfileTimer("Total"));
    m_timers.push_back(sys::ProfileTimer("GetNextDataToSend"));
    m_timers.push_back(sys::ProfileTimer("GetConnection"));
    m_timers.push_back(sys::ProfileTimer("FCGetBufferAndCpy"));
    m_timers.push_back(sys::ProfileTimer("FCSend"));
    m_timers.push_back(sys::ProfileTimer("FCPoll"));
    m_timers.push_back(sys::ProfileTimer("DataGetBuffer"));
    m_timers.push_back(sys::ProfileTimer("DataSend"));
    m_timers.push_back(sys::ProfileTimer("DataPoll"));
}

SendThread::~SendThread(void)
{

}

void SendThread::PrintStatistics(void)
{
    std::cout << "SendThread statistics:" <<
    std::endl <<
    "Throughput: " <<
        m_sentBytes / m_timers[0].GetTotalTime() / 1024.0 / 1024.0 <<
    " MB/sec" << std::endl <<
    "Sent data: " << m_sentBytes / 1024.0 / 1024.0 << " MB" << std::endl <<
    "Full send queue utilization: " <<
        (double) m_ibSendQueueBatchCount /
        (double) m_ibSendQueueFullUtilizationCount << std::endl <<
    "Process buffer utilization: " << (double) m_timers[6].GetCounter() /
        (double) m_timers[1].GetCounter() << std::endl <<
    "FC Throughput: " <<
        m_sentFlowControlBytes / m_timers[0].GetTotalTime() / 1024.0 / 1024.0 <<
    " MB/sec" << std::endl <<
    "FC Sent data: " <<
        m_sentFlowControlBytes / 1024.0 / 1024.0 << " MB" << std::endl;

    for (auto& it : m_timers) {
        std::cout << it << std::endl;
    }
}

void SendThread::_BeforeRunLoop(void)
{
    m_timers[0].Enter();
}

void SendThread::_RunLoop(void)
{
    m_timers[1].Enter();

    SendHandler::NextWorkParameters* data = m_sendHandler->GetNextDataToSend(
        m_prevNodeIdWritten, m_prevDataWritten);

    m_timers[1].Exit();

    // reset previous state
    m_prevNodeIdWritten = core::IbNodeId::INVALID;
    m_prevDataWritten = 0;

    // nothing to process
    if (data == nullptr) {
        if (!m_waitTimer.IsRunning()) {
            m_waitTimer.Start();
        }

        if (m_waitTimer.GetTimeMs() > 100.0) {
            std::this_thread::yield();
        } else if (m_waitTimer.GetTimeMs() > 1000.0) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(1));
        }

        return;
    }

    m_waitTimer.Stop();

    // seems like we got something to process
    m_prevNodeIdWritten = data->m_nodeId;

    m_timers[2].Enter();

    std::shared_ptr<core::IbConnection> connection =
        m_connectionManager->GetConnection(data->m_nodeId);

    m_timers[2].Exit();

    // connection closed in the meantime
    if (!connection) {
        // sent back to java space on the next GetNext call
        m_prevDataWritten = 0;
        return;
    }

    try {
        __ProcessFlowControl(connection, data);
        m_prevDataWritten = __ProcessBuffer(connection, data);

        m_connectionManager->ReturnConnection(connection);
    } catch (core::IbQueueClosedException& e) {
        m_connectionManager->ReturnConnection(connection);
        // ignore
    } catch (core::IbDisconnectedException& e) {
        m_connectionManager->ReturnConnection(connection);
        m_connectionManager->CloseConnection(connection->GetRemoteNodeId(),
            true);
    }
}

void SendThread::_AfterRunLoop(void)
{
    m_timers[0].Exit();
    PrintStatistics();
}

uint32_t SendThread::__ProcessFlowControl(
        std::shared_ptr<core::IbConnection>& connection,
        SendHandler::NextWorkParameters* data)
{
    const uint32_t numBytesToSend = sizeof(uint32_t);

    if (data->m_flowControlData == 0) {
        return 0;
    }

    m_timers[3].Enter();

    core::IbMemReg* mem = m_buffers->GetFlowControlBuffer(
        connection->GetConnectionId());

    memcpy(mem->GetAddress(), &data->m_flowControlData, numBytesToSend);

    m_timers[3].Exit();

    m_timers[4].Enter();

    connection->GetQp(1)->GetSendQueue()->Send(mem, 0, numBytesToSend);

    m_timers[4].Exit();

    m_timers[5].Enter();

    try {
        connection->GetQp(1)->GetSendQueue()->PollCompletion(true);
    } catch (...) {
        m_timers[5].Exit();
        throw;
    }

    m_timers[5].Exit();

    m_sentFlowControlBytes += numBytesToSend;

    return data->m_flowControlData;
}

uint32_t SendThread::__ProcessBuffer(
        std::shared_ptr<core::IbConnection>& connection,
        SendHandler::NextWorkParameters* data)
{
    uint32_t totalBytesSent = 0;

    // we are expecting the ring buffer (in java) to handle overflows
    // and slice them correctly, i.e. posFrontRel <= posBackRel, always
    // sanity check that
    if (data->m_posFrontRel > data->m_posBackRel) {
        IBNET_LOG_PANIC("posFrontRel {} > posBackRel {} not allowed",
            data->m_posFrontRel, data->m_posBackRel);
        return 0;
    }

    // happens if another send thread was able to process everything while
    // some application thread was still adding more to the queue
    if (data->m_posFrontRel == data->m_posBackRel) {
        return 0;
    }

    m_timers[6].Enter();

    core::IbMemReg* sendBuffer =
        m_buffers->GetBuffer(connection->GetConnectionId());

    m_timers[6].Exit();

    uint16_t queueSize = connection->GetQp(0)->GetSendQueue()->GetQueueSize();
    uint32_t posFront = data->m_posFrontRel;
    uint32_t posBack = data->m_posBackRel;

    while (posFront != posBack) {
        uint16_t sliceCount = 0;
        uint32_t iterationBytesSent = 0;

        // slice area of send buffer into slices fitting receive buffers
        while (sliceCount < queueSize && posFront != posBack) {
            // fits a full receive buffer
            if (posFront + m_recvBufferSize <= posBack) {
                m_timers[7].Enter();

                connection->GetQp(0)->GetSendQueue()->Send(sendBuffer,
                    posFront, m_recvBufferSize);

                m_timers[7].Exit();

                posFront += m_recvBufferSize;
                iterationBytesSent += m_recvBufferSize;
            } else {
                // smaller than a receive buffer
                m_timers[7].Enter();

                uint32_t size = posBack - posFront;

                connection->GetQp(0)->GetSendQueue()->Send(sendBuffer,
                    posFront, size);

                m_timers[7].Exit();

                posFront += size;
                iterationBytesSent += size;
            }

            sliceCount++;
        }

        // poll completions
        for (uint16_t i = 0; i < sliceCount; i++) {
            m_timers[8].Enter();

            try {
                connection->GetQp(0)->GetSendQueue()->PollCompletion(true);
            } catch (...) {
                m_timers[8].Exit();
                throw;
            }

            m_timers[8].Exit();
        }

        m_sentBytes += iterationBytesSent;
        totalBytesSent += iterationBytesSent;

        m_ibSendQueueBatchCount += sliceCount;
        m_ibSendQueueFullUtilizationCount += queueSize;
    }

    return totalBytesSent;
}

}
}