/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxcompute.bench;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxcompute.DXComputeMessageTypes;
import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.dxcompute.ms.Task;
import de.hhu.bsinfo.dxcompute.ms.TaskContext;
import de.hhu.bsinfo.dxram.chunk.ChunkIDRangeUtils;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Task to benchmark/test end-to-end communication of two nodes using the network interface
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 25.01.2017
 */
public class NetworkEndToEndTask implements Task, MessageReceiver {
    private static final Logger LOGGER = LogManager.getFormatterLogger(NetworkEndToEndTask.class.getSimpleName());

    private volatile AtomicBoolean isFinished = new AtomicBoolean(false);
    private volatile AtomicLong m_receivedCnt = new AtomicLong(0);

    private boolean m_isFirstMessage = true;
    private long m_receiveTimeStart, m_receiveTimeEnd;

    @Expose
    private int m_messageCnt = 100;
    @Expose
    private int m_messageSize = 1000;
    @Expose
    private int m_threadCnt = 10;

    @Override
    public int execute(TaskContext p_ctx) {

        short[] slaveNodeIds = p_ctx.getCtxData().getSlaveNodeIds();
        short ownSlaveID = p_ctx.getCtxData().getSlaveId();
        if (slaveNodeIds.length % 2 != 0 || slaveNodeIds.length == 0) {
            System.out.println("The number of slave have to be a multiple of two to execute this task");
            return -1;
        }

        NetworkService networkService = p_ctx.getDXRAMServiceAccessor().getService(NetworkService.class);
        networkService.registerReceiver(NetworkTestMessage.class, this);
        networkService.registerMessageType(DXComputeMessageTypes.BENCH_MESSAGE_TYPE, BenchMessages.NETWORK_TEST_MESSAGE, NetworkTestMessage.class);

        // get Messages per Thread and destination node id
        long[] messagesPerThread = ChunkIDRangeUtils.distributeChunkCountsToThreads(m_messageCnt, m_threadCnt);
        short sendNodeId;
        if (ownSlaveID % 2 == 0)
            sendNodeId = slaveNodeIds[(ownSlaveID + 1)];
        else
            sendNodeId = slaveNodeIds[(ownSlaveID - 1)];

        Thread[] threads = new Thread[m_threadCnt];
        long[] timeStart = new long[m_threadCnt];
        long[] timeEnd = new long[m_threadCnt];

        for (int i = 0; i < threads.length; i++) {
            int threadIdx = i;
            long messagesToSend = messagesPerThread[threadIdx];

            threads[i] = new Thread(() -> {

                timeStart[threadIdx] = System.nanoTime();
                NetworkTestMessage message = new NetworkTestMessage(sendNodeId, m_messageSize);

                for (int j = 0; j < messagesToSend; j++) {

                    try {
                        networkService.sendMessage(message);
                    } catch (NetworkException e) {
                        e.printStackTrace();
                    }
                }

                timeEnd[threadIdx] = System.nanoTime();
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
            return -2;
        }

        System.out.print("Times per thread:");
        for (int i = 0; i < m_threadCnt; i++) {
            System.out.printf("\nThread-%d: %f sec", i, (timeEnd[i] - timeStart[i]) / 1000.0 / 1000.0 / 1000.0);
        }
        System.out.println();

        // total time is measured by the slowest thread
        long totalTime = 0;
        for (int i = 0; i < m_threadCnt; i++) {
            long time = timeEnd[i] - timeStart[i];
            if (time > totalTime) {
                totalTime = time;
            }
        }

        System.out.printf("Total time: %f sec\n", totalTime / 1000.0 / 1000.0 / 1000.0);
        double throughput = (m_messageCnt * m_messageSize / (totalTime / 1000.0 / 1000.0 / 1000.0));
        if (throughput > 1000000)
            System.out.printf("Throughput Tx: %f MB/s", throughput / 1000.0 / 1000.0);
        else if (throughput > 1000)
            System.out.printf("Throughput Tx: %f KB/s", throughput / 1000.0);
        else
            System.out.printf("Throughput Tx: %f B/s", throughput);

        while (!isFinished.get())
            ;

        double sizeInMB = (m_messageCnt * m_messageSize) / 1000.0 / 1000.0;
        double timeInS = (m_receiveTimeEnd - m_receiveTimeStart) / 1000.0 / 1000.0 / 1000.0;
        System.out.printf("Throughput Rx: %f MB/s\n", sizeInMB / timeInS);

        return 0;
    }

    @Override
    public void handleSignal(Signal p_signal) {

    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(m_messageCnt);
        p_exporter.writeInt(m_messageSize);
        p_exporter.writeInt(m_threadCnt);
    }

    @Override
    public void importObject(Importer p_importer) {
        m_messageCnt = p_importer.readInt();
        m_messageSize = p_importer.readInt();
        m_threadCnt = p_importer.readInt();
    }

    @Override
    public int sizeofObject() {
        return 3 * Integer.BYTES;
    }

    @Override
    public void onIncomingMessage(AbstractMessage p_message) {
        if (m_isFirstMessage) {
            m_receiveTimeStart = System.nanoTime();
            m_isFirstMessage = false;
        }

        m_receivedCnt.incrementAndGet();

        if (m_receivedCnt.get() == m_messageCnt) {
            m_receiveTimeEnd = System.nanoTime();
            isFinished.compareAndSet(false, true);
        }
    }
}
