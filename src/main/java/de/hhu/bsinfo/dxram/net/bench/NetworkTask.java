/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.net.bench;

import java.util.concurrent.atomic.AtomicLong;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.chunk.bench.ChunkTaskUtils;
import de.hhu.bsinfo.dxram.ms.Task;
import de.hhu.bsinfo.dxram.ms.Signal;
import de.hhu.bsinfo.dxram.ms.TaskContext;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.net.messages.NetworkDebugMessage;
import de.hhu.bsinfo.dxram.net.messages.NetworkMessages;
import de.hhu.bsinfo.dxram.net.messages.NetworkTestMessage;
import de.hhu.bsinfo.dxram.net.messages.NetworkTestRequest;
import de.hhu.bsinfo.dxram.net.messages.NetworkTestResponse;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Task to benchmark/test network communication of nodes using the network interface
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 04.04.2017
 */
public class NetworkTask implements Task, MessageReceiver {
    private static final Logger LOGGER = LogManager.getFormatterLogger(NetworkTask.class);

    private static final int PATTERN_END_TO_END = 0;
    private static final int PATTERN_BROADCAST = 1;

    private AtomicLong m_receivedCnt = new AtomicLong(0);
    private AtomicLong m_receiveTimeStart = new AtomicLong(0);
    private AtomicLong m_receiveTimeEnd = new AtomicLong(0);

    private int m_slaveCnt;

    private NetworkService m_networkService;

    @Expose
    private long m_messageCnt = 100;
    @Expose
    private int m_messageSize = 1000;
    @Expose
    private int m_threadCnt = 10;
    @Expose
    private int m_pattern = PATTERN_END_TO_END;
    @Expose
    private boolean m_isMessage = true;
    @Expose
    private boolean m_debugMessage = false;

    @Override
    public int execute(final TaskContext p_ctx) {
        short[] slaveNodeIds = p_ctx.getCtxData().getSlaveNodeIds();
        m_slaveCnt = slaveNodeIds.length;
        short ownSlaveID = p_ctx.getCtxData().getSlaveId();

        if (m_pattern == 1 && slaveNodeIds.length <= 1) {
            System.out.println("Required number of slaves: >= 2");
            return -1;
        }

        if (m_pattern == 0 && (slaveNodeIds.length % 2 != 0 || slaveNodeIds.length == 0)) {
            System.out.println("Num slaves % 2 != 0");
            return -2;
        }

        m_networkService = p_ctx.getDXRAMServiceAccessor().getService(NetworkService.class);

        registerReceiverAndMessageTypes();

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // pre create messages/requests to use pooling
        Message[] messages = new Message[m_slaveCnt];
        for (int i = 0; i < m_slaveCnt; i++) {
            if (m_debugMessage) {
                messages[i] = new NetworkDebugMessage(slaveNodeIds[i]);
            } else {
                messages[i] = new NetworkTestMessage(slaveNodeIds[i], m_messageSize);
            }
        }

        // get Messages per Thread and destination node id
        long[] messagesPerThread = ChunkTaskUtils.distributeChunkCountsToThreads(m_messageCnt, m_threadCnt);

        Thread[] threads = new Thread[m_threadCnt];
        LatencyStatistics latencyStatistics = new LatencyStatistics(m_threadCnt);

        if (m_debugMessage) {
            System.out.printf("!!! DEBUG !!! Network benchmark, pattern %d, message count %d, %d thread(s)...\n",
                    m_pattern, m_messageCnt, m_threadCnt);
        } else {
            System.out.printf("Network benchmark, pattern %d, message count %d, message size %d byte, " +
                            " isMessages %b with %d thread(s)...\n", m_pattern, m_messageCnt, m_messageSize, m_isMessage,
                    m_threadCnt);
        }

        // thread runnables
        for (int i = 0; i < threads.length; i++) {
            int threadIdx = i;
            long messagesToSend = messagesPerThread[threadIdx];

            threads[i] = new Thread(() -> {
                switch (m_pattern) {
                    case PATTERN_END_TO_END:
                        // find index of receiver node in the slaveNodeIds array
                        int sendNodeIdIdx;
                        if (ownSlaveID % 2 == 0) {
                            sendNodeIdIdx = ownSlaveID + 1;
                        } else {
                            sendNodeIdIdx = ownSlaveID - 1;
                        }
                        for (int j = 0; j < messagesToSend; j++) {
                            try {
                                if (m_isMessage) {
                                    latencyStatistics.enter(threadIdx);
                                    m_networkService.sendMessage(messages[sendNodeIdIdx]);
                                    latencyStatistics.exit(threadIdx);
                                } else {
                                    latencyStatistics.enter(threadIdx);
                                    NetworkTestRequest request = new NetworkTestRequest(slaveNodeIds[sendNodeIdIdx]);
                                    m_networkService.sendSync(request);
                                    latencyStatistics.exit(threadIdx);
                                }
                            } catch (final NetworkException e) {
                                LOGGER.error("Sending message failed", e);
                            }
                        }
                        break;

                    case PATTERN_BROADCAST:
                        for (int j = 0; j < messagesToSend; j++) {
                            // send message to every slave
                            for (int k = 0; k < m_slaveCnt; k++) {
                                if (k == ownSlaveID) {
                                    continue;
                                }

                                try {
                                    if (m_isMessage) {
                                        latencyStatistics.enter(threadIdx);
                                        m_networkService.sendMessage(messages[k]);
                                        latencyStatistics.exit(threadIdx);
                                    } else {
                                        NetworkTestRequest request = new NetworkTestRequest(slaveNodeIds[k]);
                                        latencyStatistics.enter(threadIdx);
                                        m_networkService.sendSync(request);
                                        latencyStatistics.exit(threadIdx);
                                    }
                                } catch (final NetworkException e) {
                                    LOGGER.error("Sending message failed", e);
                                }
                            }
                        }
                        break;

                    default:
                        throw new RuntimeException("Unsupported pattern");
                }
            });
        }

        for (Thread t : threads) {
            t.start();
        }

        boolean threadJoinFailed = false;
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (final InterruptedException e) {
                LOGGER.error("Joining thread failed", e);
                threadJoinFailed = true;
            }
        }

        if (threadJoinFailed) {
            return -3;
        }

        System.out.print("Times per thread:");
        for (int i = 0; i < m_threadCnt; i++) {
            System.out.printf("\nThread-%d: %f sec", i, latencyStatistics.getTotalTime(i).getSecDouble());
        }
        System.out.println();

        // total time is measured by the slowest thread
        long totalTime = 0;
        for (int i = 0; i < m_threadCnt; i++) {
            long t = latencyStatistics.getTotalTime(i).getNs();
            if (t > totalTime) {
                totalTime = t;
            }
        }
        System.out.printf("Total time: %f sec\n", totalTime / 1000.0 / 1000.0 / 1000.0);
        double sizeInMB;
        if (m_pattern == PATTERN_BROADCAST) {
            sizeInMB = m_messageCnt * m_messageSize * (m_slaveCnt - 1) / 1000.0 / 1000.0;
        } else {
            sizeInMB = m_messageCnt * m_messageSize / 1000.0 / 1000.0;
        }
        double timeInS = totalTime / 1000.0 / 1000.0 / 1000.0;
        double throughput = sizeInMB / timeInS;
        System.out.printf("Throughput Tx: %f MB/s\n", throughput);

        while (m_receiveTimeEnd.get() == 0) {
            // wait until all messages / requests received
            Thread.yield();
        }

        // calculate receive throughput
        timeInS = (m_receiveTimeEnd.get() - m_receiveTimeStart.get()) / 1000.0 / 1000.0 / 1000.0;
        System.out.printf("Throughput Rx: %f MB/s\n", sizeInMB / timeInS);

        System.out.println("Request-Response/Message latencies:\n" + latencyStatistics);

        unregisterReceiver();

        return 0;
    }

    @Override
    public void handleSignal(final Signal p_signal) {
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(m_messageCnt);
        p_exporter.writeInt(m_messageSize);
        p_exporter.writeInt(m_threadCnt);
        p_exporter.writeInt(m_pattern);
        p_exporter.writeBoolean(m_isMessage);
        p_exporter.writeBoolean(m_debugMessage);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_messageCnt = p_importer.readLong(m_messageCnt);
        m_messageSize = p_importer.readInt(m_messageSize);
        m_threadCnt = p_importer.readInt(m_threadCnt);
        m_pattern = p_importer.readInt(m_pattern);
        m_isMessage = p_importer.readBoolean(m_isMessage);
        m_debugMessage = p_importer.readBoolean(m_debugMessage);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES + 3 * Integer.BYTES + ObjectSizeUtil.sizeofBoolean() * 2;
    }

    @Override
    public void onIncomingMessage(final Message p_message) {

        LOGGER.trace("Entering incomingMessage with: p_message=%s", p_message);

        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.NETWORK_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case NetworkMessages.SUBTYPE_TEST_MESSAGE:
                        incomingNetworkTestMessage((NetworkTestMessage) p_message);
                        break;
                    case NetworkMessages.SUBTYPE_TEST_REQUEST:
                        incomingNetworkTestRequest((NetworkTestRequest) p_message);
                        break;
                    case NetworkMessages.SUBTYPE_DEBUG_MESSAGE:
                        incomingNetworkDebugMessage((NetworkDebugMessage) p_message);
                        break;
                    default:
                        break;
                }
            }
        }

        LOGGER.trace("Exiting incomingMessage");

    }

    private void incomingNetworkTestRequest(final NetworkTestRequest p_request) {
        m_receiveTimeStart.compareAndSet(0, System.nanoTime());

        m_receivedCnt.incrementAndGet();

        long numOfMessagesToReceive = m_pattern == 0 ? m_messageCnt : m_messageCnt * (m_slaveCnt - 1);
        if (m_receivedCnt.get() == numOfMessagesToReceive) {
            m_receiveTimeEnd.compareAndSet(0, System.nanoTime());
        }

        NetworkTestResponse response = new NetworkTestResponse(p_request, m_messageSize);
        try {
            m_networkService.sendMessage(response);
        } catch (final NetworkException e) {

            LOGGER.error("Sending NetworkTestResponse for %s failed: %s", p_request, e);

        }
    }

    private void incomingNetworkTestMessage(final NetworkTestMessage p_message) {
        m_receiveTimeStart.compareAndSet(0, System.nanoTime());

        m_receivedCnt.incrementAndGet();

        long numOfMessagesToReceive = m_pattern == 0 ? m_messageCnt : m_messageCnt * (m_slaveCnt - 1);
        if (m_receivedCnt.get() == numOfMessagesToReceive) {
            m_receiveTimeEnd.compareAndSet(0, System.nanoTime());
        }
    }

    private void incomingNetworkDebugMessage(final NetworkDebugMessage p_message) {
        m_receiveTimeStart.compareAndSet(0, System.nanoTime());

        m_receivedCnt.incrementAndGet();

        long numOfMessagesToReceive = m_pattern == 0 ? m_messageCnt : m_messageCnt * (m_slaveCnt - 1);
        if (m_receivedCnt.get() == numOfMessagesToReceive) {
            m_receiveTimeEnd.compareAndSet(0, System.nanoTime());
        }
    }

    private void registerReceiverAndMessageTypes() {
        m_networkService.registerReceiver(DXRAMMessageTypes.NETWORK_MESSAGES_TYPE, NetworkMessages.SUBTYPE_TEST_MESSAGE,
                this);
        m_networkService.registerReceiver(DXRAMMessageTypes.NETWORK_MESSAGES_TYPE, NetworkMessages.SUBTYPE_TEST_REQUEST,
                this);
        m_networkService.registerReceiver(DXRAMMessageTypes.NETWORK_MESSAGES_TYPE,
                NetworkMessages.SUBTYPE_DEBUG_MESSAGE, this);
        m_networkService.registerMessageType(DXRAMMessageTypes.NETWORK_MESSAGES_TYPE,
                NetworkMessages.SUBTYPE_TEST_MESSAGE, NetworkTestMessage.class);
        m_networkService.registerMessageType(DXRAMMessageTypes.NETWORK_MESSAGES_TYPE,
                NetworkMessages.SUBTYPE_TEST_REQUEST, NetworkTestRequest.class);
        m_networkService.registerMessageType(DXRAMMessageTypes.NETWORK_MESSAGES_TYPE,
                NetworkMessages.SUBTYPE_TEST_RESPONSE, NetworkTestResponse.class);
        m_networkService.registerMessageType(DXRAMMessageTypes.NETWORK_MESSAGES_TYPE,
                NetworkMessages.SUBTYPE_DEBUG_MESSAGE, NetworkDebugMessage.class);
    }

    private void unregisterReceiver() {
        m_networkService.unregisterReceiver(DXRAMMessageTypes.NETWORK_MESSAGES_TYPE,
                NetworkMessages.SUBTYPE_TEST_MESSAGE, this);
        m_networkService.unregisterReceiver(DXRAMMessageTypes.NETWORK_MESSAGES_TYPE,
                NetworkMessages.SUBTYPE_TEST_REQUEST, this);
        m_networkService.unregisterReceiver(DXRAMMessageTypes.NETWORK_MESSAGES_TYPE,
                NetworkMessages.SUBTYPE_DEBUG_MESSAGE, this);
    }
}
