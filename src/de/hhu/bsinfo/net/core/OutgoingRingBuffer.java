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

package de.hhu.bsinfo.net.core;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The MessageCreator builds messages and forwards them to the MessageHandlers.
 * Uses a ring-buffer implementation for incoming buffers.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 31.05.2016
 */
public final class OutgoingRingBuffer {

    // Attributes
    private final byte[] m_buffer;
    private volatile int m_posFront;
    private AtomicInteger m_posBack;

    private AtomicInteger m_producers;
    private volatile boolean m_consumerWaits;

    private ByteBuffer m_sendByteBuffer;

    private boolean m_directBuffer;

    /**
     * Creates an instance of MessageCreator
     *
     * @param p_osBufferSize
     *         the outgoing buffer size
     */
    OutgoingRingBuffer(final int p_osBufferSize, final boolean p_directBuffers) {
        m_directBuffer = p_directBuffers;

        m_buffer = new byte[p_osBufferSize * 2];

        m_posFront = 0;
        m_posBack = new AtomicInteger(0);

        m_producers = new AtomicInteger(0);
        m_consumerWaits = false;

        //m_sendBuffer = new Buffer(m_buffer);
        m_sendByteBuffer = ByteBuffer.wrap(m_buffer);
    }

    /**
     * Returns whether the ring-buffer is empty or not.
     *
     * @return whether the ring-buffer is empty or not
     */
    public boolean isEmpty() {
        return (m_posFront & 0x7FFFFFFFL) == (m_posBack.get() & 0x7FFFFFFFL);
    }

    /**
     * Gets a buffer from the beginning of the array.
     *
     * @return the ByteBuffer
     */
    public ByteBuffer popFront() {
        int posBack;
        int posBackRelative;
        int posFront;
        int posFrontRelative;
        int bufferSize = m_buffer.length;

        // Deny access to incoming producers
        m_consumerWaits = true;

        // Wait for all producers to finish copying
        while (m_producers.get() > 0) {
            Thread.yield();
        }

        posBack = (int) (m_posBack.get() & 0x7FFFFFFFL);
        posBackRelative = posBack % bufferSize;
        posFront = (int) (m_posFront & 0x7FFFFFFFL);
        posFrontRelative = posFront % bufferSize;
        if (posBackRelative < posFrontRelative) {
            m_sendByteBuffer.limit(bufferSize);
        } else {
            m_sendByteBuffer.limit(posBackRelative);
        }
        m_sendByteBuffer.position(posFrontRelative);

        m_consumerWaits = false;
        // TODO: expensive to enter synchronized block
        synchronized (m_buffer) {
            m_buffer.notifyAll();
        }

        return m_sendByteBuffer;
    }

    public void shiftFront(final int p_writtenBytes) {
        m_posFront += p_writtenBytes;
    }

    boolean pushNodeID(final ByteBuffer p_buffer) {

        if (m_posBack.get() == 0) {
            m_buffer[0] = p_buffer.get();
            m_buffer[1] = p_buffer.get();
            m_posBack.set(2);
        } else {
            System.out.println("ERROR");
        }

        return true;
    }

    /**
     * Serializes, adds and aggregates a message at the end of the array.
     *
     * @param p_message
     *         the outgoing message
     * @return whether the queue was empty or not
     * @throws NetworkException
     *         if message could not be serialized
     */
    boolean pushMessage(final AbstractMessage p_message, final int p_messageSize) throws NetworkException {
        int posBackUnlimited;
        int posBack;
        int posBackRelative;
        int posFront;
        int posFrontRelative;
        int bufferSize = m_buffer.length;

        m_producers.incrementAndGet();
        while (m_consumerWaits) {
            // Consumer wants to flush
            m_producers.decrementAndGet();
            while (m_consumerWaits) {
                try {
                    synchronized (m_buffer) {
                        m_buffer.wait();
                    }
                } catch (InterruptedException ignore) {
                }
            }
            m_producers.incrementAndGet();
        }

        // Allocate space in ring buffer by incrementing position back
        while (true) {
            posBackUnlimited = m_posBack.get(); // We need this value for compareAndSet operation
            posBack = (int) (posBackUnlimited & 0x7FFFFFFFL); // posBack must not be negative
            posBackRelative = posBack % bufferSize; // posBackRelative must not exceed buffer
            posFront = (int) (m_posFront & 0x7FFFFFFFL);
            posFrontRelative = posFront % bufferSize;

            if (posBack == posFront /* empty */ ||
                    posBackRelative < posFrontRelative && posFrontRelative - posBackRelative > p_messageSize /* with overflow */ ||
                    posBackRelative > posFrontRelative && bufferSize - posBackRelative + posFrontRelative > p_messageSize /* without overflow */) {

                // Optimistic increase
                if (m_posBack.compareAndSet(posBackUnlimited, posBackUnlimited + p_messageSize)) {
                    // Position back could be set
                    break;
                }
                // Try again
            } else {
                // Buffer is full -> wait
                System.out.println("Buffer full!");
                m_producers.decrementAndGet();
                try {
                    synchronized (m_buffer) {
                        m_buffer.wait();
                    }
                } catch (InterruptedException ignore) {
                }
                m_producers.incrementAndGet();
            }
        }

        // Serialize message into ring buffer
        int start = posBackRelative;
        if (p_messageSize > bufferSize - start /* with overflow */) {
            p_message.serialize(m_buffer, start, p_messageSize, true);
        } else {
            p_message.serialize(m_buffer, start, p_messageSize, false);
        }

        // Leave
        m_producers.decrementAndGet();

        return posFront == posBack;
    }
}
