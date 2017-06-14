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

package de.hhu.bsinfo.ethnet;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The MessageCreator builds messages and forwards them to the MessageHandlers.
 * Uses a ring-buffer implementation for incoming buffers.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 31.05.2016
 */
class OutgoingQueue {

    // Constants
    private static final int BUFFER_POOL_SIZE = 4;

    // Attributes
    private final ByteBuffer[] m_buffer;
    private int m_maxBytes;
    private int m_osBufferSize;

    private volatile int m_currentBytes;
    private volatile int m_posFront;
    private volatile int m_posBack;

    private final ByteBuffer[] m_bufferPool;
    private int m_bufferPoolIndex;

    /**
     * Creates an instance of MessageCreator
     *
     * @param p_osBufferSize
     *     the outgoing buffer size
     */
    OutgoingQueue(final int p_osBufferSize) {
        m_osBufferSize = p_osBufferSize;

        m_buffer = new ByteBuffer[BUFFER_POOL_SIZE];
        m_maxBytes = 2 * p_osBufferSize;
        m_currentBytes = 0;

        m_bufferPool = new ByteBuffer[BUFFER_POOL_SIZE];
        for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
            m_bufferPool[i] = ByteBuffer.allocate(p_osBufferSize);
        }
        m_bufferPoolIndex = BUFFER_POOL_SIZE - 1;

        m_posFront = 0;
        m_posBack = 0;
    }

    /**
     * Returns whether the ring-buffer is empty or not.
     *
     * @return whether the ring-buffer is empty or not
     */
    public boolean isEmpty() {
        return m_currentBytes == 0;
    }

    /**
     * Returns whether the ring-buffer is full or not.
     *
     * @return whether the ring-buffer is full or not
     */
    public boolean isFull() {
        return (m_posBack + 2) % BUFFER_POOL_SIZE == m_posFront % BUFFER_POOL_SIZE || (m_posBack + 1) % BUFFER_POOL_SIZE == m_posFront % BUFFER_POOL_SIZE;
    }

    /**
     * Returns the number of pending bytes.
     *
     * @return the number of pending bytes
     */
    protected int bytes() {
        return m_currentBytes;
    }

    /**
     * Returns the number of pending buffers.
     *
     * @return the number of pending buffers
     */
    protected int size() {
        return m_posBack - m_posFront;
    }

    /**
     * Gets a buffer from the beginning of the array.
     *
     * @return the ByteBuffer
     */
    ByteBuffer popFront() {
        ByteBuffer ret;

        if (m_posFront == m_posBack) {
            // Ring-buffer is empty.
            return null;
        }

        synchronized (m_buffer) {
            ret = m_buffer[m_posFront % BUFFER_POOL_SIZE];
            m_posFront++;

            m_currentBytes -= ret.remaining();

            m_buffer.notifyAll();
        }

        return ret;
    }

    /**
     * Adds a buffer at the beginning of the array.
     *
     * @param p_buffer
     *     the outgoing buffer
     * @return whether the job was added or not
     */
    boolean pushFront(final ByteBuffer p_buffer) {
        synchronized (m_buffer) {
            if (m_posFront == 0) {
                m_buffer[0] = p_buffer;
                m_posBack++;
            } else{
                m_buffer[(m_posFront - 1) % BUFFER_POOL_SIZE] = p_buffer;
                m_posFront--;
            }
            m_currentBytes += p_buffer.remaining();
        }

        return true;
    }

    void returnBuffer(ByteBuffer p_buffer) {
        p_buffer.clear();
        synchronized (m_bufferPool) {
            if (m_bufferPoolIndex < BUFFER_POOL_SIZE - 1) {
                m_bufferPool[++m_bufferPoolIndex] = p_buffer;
            }
        }
    }

    /**
     * Adds and aggregates a buffer at the end of the array.
     *
     * @param p_buffer
     *     the outgoing buffer
     * @return whether the queue was empty or not
     */
    boolean pushAndAggregateBuffers(ByteBuffer p_buffer) {
        boolean ret;
        ByteBuffer buf;

        while ((m_posBack + 2) % BUFFER_POOL_SIZE == m_posFront % BUFFER_POOL_SIZE || (m_posBack + 1) % BUFFER_POOL_SIZE == m_posFront % BUFFER_POOL_SIZE
                || m_currentBytes >= m_maxBytes) {
            try {
                synchronized (m_buffer) {
                    m_buffer.wait();
                }
            } catch (InterruptedException ignore) {}
        }


        int counter = 0;
        int size = p_buffer.remaining();

        synchronized (m_buffer) {
            ret = m_currentBytes == 0;
            while (counter < m_posBack - m_posFront) {
                ByteBuffer buffer = m_buffer[(m_posBack - counter - 1) % BUFFER_POOL_SIZE];
                int newSize = size + buffer.remaining();
                if (newSize <= m_osBufferSize) {
                    size = newSize;
                    counter++;
                } else {
                    break;
                }
            }

            if (counter == 0) {
                // The queue is empty or last entry is too large -> append entry solely
                push(p_buffer);
            } else {
                int counterBackup = counter;
                int oldPos = 0;

                ByteBuffer firstBuffer = m_buffer[(m_posBack - counter) % BUFFER_POOL_SIZE];
                if (firstBuffer.capacity() - firstBuffer.position() >= size) {
                    oldPos = firstBuffer.position();
                    firstBuffer.position(firstBuffer.limit());
                    firstBuffer.limit(firstBuffer.capacity());
                    buf = firstBuffer;
                    counter--;
                } else {
                    synchronized (m_bufferPool) {
                        buf = m_bufferPool[m_bufferPoolIndex--];
                    }
                }
                while (counter > 0) {
                    ByteBuffer buffer = m_buffer[(m_posBack - counter) % BUFFER_POOL_SIZE];
                    buf.put(buffer);

                    if (buffer.capacity() == m_osBufferSize) {
                        returnBuffer(buffer);
                    }
                    counter--;
                }
                m_currentBytes += p_buffer.remaining();
                buf.put(p_buffer);
                buf.limit(buf.position());
                buf.position(oldPos);

                m_posBack -= counterBackup;
                m_buffer[m_posBack % BUFFER_POOL_SIZE] = buf;
                m_posBack++;
            }
        }

        return ret;
    }

    /**
     * Serializes, adds and aggregates a message at the end of the array.
     *
     * @param p_message
     *     the outgoing message
     * @return whether the queue was empty or not
     * @throws NetworkException if message could not be serialized
     */
    boolean pushAndAggregateBuffers(AbstractMessage p_message, final int p_messageSize) throws NetworkException {
        boolean ret;
        ByteBuffer buf;

        while ((m_posBack + 2) % BUFFER_POOL_SIZE == m_posFront % BUFFER_POOL_SIZE || (m_posBack + 1) % BUFFER_POOL_SIZE == m_posFront % BUFFER_POOL_SIZE
                || m_currentBytes >= m_maxBytes) {
            try {
                synchronized (m_buffer) {
                    m_buffer.wait();
                }
            } catch (InterruptedException ignore) {}
        }

        int counter = 0;
        int size = p_messageSize;

        synchronized (m_buffer) {
            ret = m_currentBytes == 0;
            while (counter < m_posBack - m_posFront) {
                ByteBuffer buffer = m_buffer[(m_posBack - counter - 1) % BUFFER_POOL_SIZE];
                int newSize = size + buffer.remaining();
                if (newSize <= m_osBufferSize) {
                    size = newSize;
                    counter++;
                } else {
                    break;
                }
            }

            if (counter == 0) {
                // The queue is empty or last entry is too large -> append entry solely
                push(p_message, p_messageSize);
            } else {
                int counterBackup = counter;
                int oldPos = 0;

                ByteBuffer firstBuffer = m_buffer[(m_posBack - counter) % BUFFER_POOL_SIZE];
                if (firstBuffer.capacity() - firstBuffer.position() >= size) {
                    oldPos = firstBuffer.position();
                    firstBuffer.position(firstBuffer.limit());
                    firstBuffer.limit(firstBuffer.capacity());
                    buf = firstBuffer;
                    counter--;
                } else {
                    synchronized (m_bufferPool) {
                        buf = m_bufferPool[m_bufferPoolIndex--];
                    }
                }
                while (counter > 0) {
                    ByteBuffer buffer = m_buffer[(m_posBack - counter) % BUFFER_POOL_SIZE];
                    buf.put(buffer);

                    if (buffer.capacity() == m_osBufferSize) {
                        returnBuffer(buffer);
                    }
                    counter--;
                }
                m_currentBytes += p_messageSize;
                try {
                    p_message.serialize(buf, p_messageSize);
                    buf.limit(buf.position());
                    buf.position(oldPos);
                } catch (final NetworkException e) {
                    m_posBack -= counterBackup - p_messageSize;
                    m_buffer[m_posBack % BUFFER_POOL_SIZE] = buf;
                    m_posBack++;

                    throw e;
                }

                m_posBack -= counterBackup;
                m_buffer[m_posBack % BUFFER_POOL_SIZE] = buf;
                m_posBack++;
            }
        }

        return ret;
    }

    /**
     * Adds a buffer at the end of the array.
     *
     * @param p_buffer
     *     the outgoing buffer
     * @lock must be locked
     */
    private void push(final ByteBuffer p_buffer) {
        m_buffer[m_posBack % BUFFER_POOL_SIZE] = p_buffer;
        m_posBack++;

        m_currentBytes += p_buffer.remaining();
    }

    /**
     * Serializes a message and appends it to the array.
     *
     * @param p_message
     *     the message
     * @throws NetworkException if message could not be serialized
     * @lock must be locked
     */
    private void push(final AbstractMessage p_message, final int p_messageSize) throws NetworkException {
            m_buffer[m_posBack % BUFFER_POOL_SIZE] = p_message.getBuffer();
            m_posBack++;

            m_currentBytes += p_messageSize;
    }
}
