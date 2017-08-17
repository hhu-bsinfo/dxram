/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxnet.core;

import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.nio.NIOBufferPool;
import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;

/**
 * The MessageCreator builds messages and forwards them to the MessageHandlers.
 * Uses a ring-buffer implementation for incoming buffers.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 31.05.2016
 */
public class MessageCreator extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MessageCreator.class.getSimpleName());
    private static final StatisticsOperation SOP_PUSH = StatisticsRecorderManager.getOperation(MessageCreator.class, "MessageCreatorPush");
    private static final StatisticsOperation SOP_POP = StatisticsRecorderManager.getOperation(MessageCreator.class, "MessageCreatorPop");

    // must be a power of two to work with wrap around
    private static final int SIZE = 2 * 2 * 1024;
    private static final int THRESHOLD_PARK = 10000;

    private volatile boolean m_shutdown;

    private AbstractConnection[] m_connectionBuffer;
    private NIOBufferPool.DirectBufferWrapper[] m_directBuffers;
    private long[] m_bufferHandleBuffer;
    private long[] m_addrBuffer;
    private int[] m_sizeBuffer;

    private int m_maxBytes;
    private long m_currentBytes;

    // single producer, single consumer lock free queue
    private volatile int m_posFront;
    private volatile int m_posBack;

    /**
     * Creates an instance of MessageCreator
     *
     * @param p_maxIncomingBufferSize
     *         the max incoming buffer size
     */
    public MessageCreator(final int p_maxIncomingBufferSize) {
        m_connectionBuffer = new AbstractConnection[SIZE];
        m_directBuffers = new NIOBufferPool.DirectBufferWrapper[SIZE];
        m_bufferHandleBuffer = new long[SIZE];
        m_addrBuffer = new long[SIZE];
        m_sizeBuffer = new int[SIZE];
        m_maxBytes = p_maxIncomingBufferSize * 8;
        m_currentBytes = 0;

        m_posFront = 0;
        m_posBack = 0;
    }

    /**
     * Returns whether the ring-buffer is empty or not.
     */
    public boolean isEmpty() {
        return m_posFront == m_posBack;
    }

    /**
     * Returns whether the ring-buffer is full or not.
     */
    public boolean isFull() {
        return (m_posBack + 2) % SIZE == m_posFront % SIZE;
    }

    /**
     * Returns the number of pending buffers.
     */
    public int size() {
        return (m_posBack - m_posFront) / 2;
    }

    @Override
    public void run() {
        AbstractConnection connection;
        AbstractPipeIn pipeIn;

        NIOBufferPool.DirectBufferWrapper buffer;
        long bufferHandle;
        long addr;
        int size;
        int parkCounter = 0;

        while (!m_shutdown) {
            // #ifdef STATISTICS
            SOP_POP.enter();
            // #endif /* STATISTICS */

            // pop a job
            if (m_posFront == m_posBack) {
                // Ring-buffer is empty.

                // Wait for a short period (~ xx µs) and continue
                // keep latency low (especially on infiniband) but also keep cpu load low
                // avoid parking on every iteration -> increases overall latency for messages
                if (parkCounter >= THRESHOLD_PARK) {
                    LockSupport.parkNanos(1);
                } else {
                    parkCounter++;
                }
            } else {
                int front = m_posFront % SIZE;

                parkCounter = 0;

                connection = m_connectionBuffer[front];
                bufferHandle = m_bufferHandleBuffer[front];
                buffer = m_directBuffers[front];
                addr = m_addrBuffer[front];
                size = m_sizeBuffer[front];

                m_currentBytes -= size;
                m_posFront++;

                // #ifdef STATISTICS
                SOP_POP.leave();
                // #endif /* STATISTICS */

                pipeIn = connection.getPipeIn();

                try {
                    pipeIn.processBuffer(addr, size);
                } catch (final NetworkException e) {
                    // #if LOGGER == ERROR
                    LOGGER.error("Processing incoming buffer failed", e);
                    // #endif /* LOGGER == ERROR */
                } finally {
                    // always return buffer. otherwise, our pool will run dry after a while on infrequent errors
                    pipeIn.returnProcessedBuffer(buffer, bufferHandle);
                }
            }
        }
    }

    /**
     * Shutdown the message creator thread
     */
    public void shutdown() {
        // #if LOGGER == INFO
        LOGGER.info("Message creator shutdown...");
        // #endif /* LOGGER == INFO */

        m_shutdown = true;

        try {
            // wait a moment for the thread to shut down (if it can)
            Thread.sleep(100);
        } catch (final InterruptedException ignore) {

        }

        interrupt();
        LockSupport.unpark(this);
        try {
            join();
        } catch (final InterruptedException ignore) {
        }
    }

    /**
     * Adds a job with connection and incoming buffer to the end of the ring buffer.
     *
     * @param p_connection
     *         the connection associated with the buffer
     * @param p_directBufferWrapper
     *         Used on NIO to wrap an incoming buffer
     * @param p_bufferHandle
     *         Implementation dependent handle identifying the buffer
     * @param p_addr
     *         (Unsafe) address to the incoming buffer
     * @param p_size
     *         Size of the incoming buffer
     * @return whether the job was added or not
     */
    public boolean pushJob(final AbstractConnection p_connection, final NIOBufferPool.DirectBufferWrapper p_directBufferWrapper, final long p_bufferHandle,
            final long p_addr, final int p_size) {
        int back;

        // #ifdef STATISTICS
        SOP_PUSH.enter();
        // #endif /* STATISTICS */

        if ((m_posBack + 1) % SIZE == m_posFront % SIZE || m_currentBytes >= m_maxBytes) {
            // Return without adding the job if queue is full or too many bytes are pending
            return false;
        }

        back = m_posBack % SIZE;

        m_connectionBuffer[back] = p_connection;
        m_directBuffers[back] = p_directBufferWrapper;
        m_bufferHandleBuffer[back] = p_bufferHandle;
        m_addrBuffer[back] = p_addr;
        m_sizeBuffer[back] = p_size;
        m_currentBytes += p_size;
        m_posBack++;

        // #ifdef STATISTICS
        SOP_PUSH.leave();
        // #endif /* STATISTICS */

        return true;
    }
}
