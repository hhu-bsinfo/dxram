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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.utils.UnsafeHandler;
import de.hhu.bsinfo.utils.stats.StatisticsOperation;
import de.hhu.bsinfo.utils.stats.StatisticsRecorderManager;

/**
 * The IncomingBufferQueue stored incoming buffers from all connections.
 * Uses a ring-buffer implementation for incoming buffers.
 * One producer (network thread) and one consumer (message creation coordinator).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 31.05.2016
 */
public class IncomingBufferQueue {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IncomingBufferQueue.class.getSimpleName());

    private static final StatisticsOperation SOP_PUSH = StatisticsRecorderManager.getOperation(IncomingBufferQueue.class, "IncomingBufferQueuePush");
    private static final StatisticsOperation SOP_POP = StatisticsRecorderManager.getOperation(IncomingBufferQueue.class, "IncomingBufferQueuePop");

    // must be a power of two to work with wrap around
    private static final int SIZE = 2 * 2 * 1024;

    private AbstractConnection[] m_connectionBuffer;
    private BufferPool.DirectBufferWrapper[] m_directBuffers;
    private long[] m_bufferHandleBuffer;
    private long[] m_addrBuffer;
    private int[] m_sizeBuffer;
    private IncomingBuffer m_incomingBuffer;

    private final int m_maxBytes;
    private AtomicInteger m_currentBytes;

    // single producer, single consumer lock free queue (posBack and posFront are synchronized with fences and byte counter)
    private int m_posBack; // 31 bits used (see incrementation)
    private int m_posFront; // 31 bits used (see incrementation)

    /**
     * Creates an instance of IncomingBufferQueue
     *
     * @param p_maxIncomingBufferSize
     *         the max incoming buffer size
     */
    IncomingBufferQueue(final int p_maxIncomingBufferSize) {
        if ((SIZE & SIZE - 1) != 0) {
            throw new NetworkRuntimeException("Incoming buffer queue size must be a power of 2!");
        }

        m_connectionBuffer = new AbstractConnection[SIZE];
        m_directBuffers = new BufferPool.DirectBufferWrapper[SIZE];
        m_bufferHandleBuffer = new long[SIZE];
        m_addrBuffer = new long[SIZE];
        m_sizeBuffer = new int[SIZE];
        m_maxBytes = p_maxIncomingBufferSize * 8;
        m_currentBytes = new AtomicInteger(0);

        m_posBack = 0;
        m_posFront = 0;

        m_incomingBuffer = new IncomingBuffer();
    }

    /**
     * Returns whether the ring-buffer is full or not.
     */
    public boolean isFull() {
        return m_currentBytes.get() >= m_maxBytes || (m_posBack + SIZE & 0x7FFFFFFF) == m_posFront;
    }

    /**
     * Removes one buffer from queue.
     */
    IncomingBuffer popBuffer() {

        // #ifdef STATISTICS
        SOP_POP.enter();
        // #endif /* STATISTICS */

        UnsafeHandler.getInstance().getUnsafe().loadFence();
        if (m_posBack == m_posFront) {
            // Empty
            return null;
        }

        int back = m_posBack % SIZE;
        int size = m_sizeBuffer[back];
        m_incomingBuffer.set(m_connectionBuffer[back].getPipeIn(), m_directBuffers[back], m_bufferHandleBuffer[back], m_addrBuffer[back], size);

        // & 0x7FFFFFFF kill sign
        m_posBack = m_posBack + 1 & 0x7FFFFFFF;
        m_currentBytes.addAndGet(-size); // Includes storeFence()

        // #ifdef STATISTICS
        SOP_POP.leave();
        // #endif /* STATISTICS */

        return m_incomingBuffer;
    }

    /**
     * Adds an incoming buffer with connection to the end of the ring buffer.
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
    public boolean pushBuffer(final AbstractConnection p_connection, final BufferPool.DirectBufferWrapper p_directBufferWrapper, final long p_bufferHandle,
            final long p_addr, final int p_size) {
        int front;

        // #ifdef STATISTICS
        SOP_PUSH.enter();
        // #endif /* STATISTICS */

        if (p_size == 0) {
            // #if LOGGER >= WARN
            LOGGER.warn("Buffer size must not be 0. Incoming buffer is discarded.");
            // #endif /* LOGGER >= WARN */

            return true;
        }

        if (m_currentBytes.get() >= m_maxBytes || (m_posBack + SIZE & 0x7FFFFFFF) == m_posFront) { // m_currentBytes.get() includes loadFence()
            // Return without adding the job if queue is full or too many bytes are pending
            return false;
        }

        front = m_posFront % SIZE;

        m_connectionBuffer[front] = p_connection;
        m_directBuffers[front] = p_directBufferWrapper;
        m_bufferHandleBuffer[front] = p_bufferHandle;
        m_addrBuffer[front] = p_addr;
        m_sizeBuffer[front] = p_size;
        // & 0x7FFFFFFF kill sign
        m_posFront = m_posFront + 1 & 0x7FFFFFFF;
        m_currentBytes.addAndGet(p_size); // Includes storeFence()

        // #ifdef STATISTICS
        SOP_PUSH.leave();
        // #endif /* STATISTICS */

        return true;
    }

    /**
     * Wrapper class to forward buffers to message creator.
     *
     * @author Kevin Beineke, kevin.beineke@hhu.de, 27.09.2017
     */
    static final class IncomingBuffer {

        AbstractPipeIn m_pipeIn;
        BufferPool.DirectBufferWrapper m_buffer;
        long m_bufferHandle;
        long m_bufferAddress;
        int m_bufferSize;

        /**
         * Creates an instance of IncomingBuffer
         */
        private IncomingBuffer() {
        }

        /**
         * Returns the pipe in
         *
         * @return AbstractPipeIn
         */
        AbstractPipeIn getPipeIn() {
            return m_pipeIn;
        }

        /**
         * Returns the native memory buffer
         *
         * @return the DirectBufferWrapper
         */
        BufferPool.DirectBufferWrapper getDirectBuffer() {
            return m_buffer;
        }

        /**
         * Returns the buffer handle
         *
         * @return the buffer handler
         */
        long getBufferHandle() {
            return m_bufferHandle;
        }

        /**
         * Returns the buffer address
         *
         * @return the address
         */
        long getBufferAddress() {
            return m_bufferAddress;
        }

        /**
         * Returns the buffer size
         *
         * @return the buffer size
         */
        int getBufferSize() {
            return m_bufferSize;
        }

        /**
         * Set all attributes prior to delivery.
         *
         * @param p_pipeIn
         *         the AbstractPipeIn
         * @param p_buffer
         *         the buffer
         * @param p_bufferHandle
         *         the buffer handle
         * @param p_bufferAddress
         *         the buffer address
         * @param p_bufferSize
         *         the buffer size
         */
        private void set(final AbstractPipeIn p_pipeIn, final BufferPool.DirectBufferWrapper p_buffer, final long p_bufferHandle, final long p_bufferAddress,
                final int p_bufferSize) {
            m_pipeIn = p_pipeIn;
            m_buffer = p_buffer;
            m_bufferHandle = p_bufferHandle;
            m_bufferAddress = p_bufferAddress;
            m_bufferSize = p_bufferSize;
        }
    }
}
