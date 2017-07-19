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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The MessageCreator builds messages and forwards them to the MessageHandlers.
 * Uses a ring-buffer implementation for incoming buffers.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 31.05.2016
 */
public abstract class AbstractOutgoingRingBuffer {

    // Attributes
    protected volatile int m_posFront;
    protected final AtomicInteger m_posBack;

    protected final AtomicInteger m_producers;
    protected volatile boolean m_consumerWaits;

    private final int m_bufferSize;

    public AbstractOutgoingRingBuffer(final int p_bufferSize) {
        m_posFront = 0;
        m_posBack = new AtomicInteger(0);

        m_producers = new AtomicInteger(0);
        m_consumerWaits = false;

        m_bufferSize = p_bufferSize;
    }

    /**
     * Returns whether the ring-buffer is empty or not.
     *
     * @return whether the ring-buffer is empty or not
     */
    public boolean isEmpty() {
        return (m_posFront & 0x7FFFFFFFL) == (m_posBack.get() & 0x7FFFFFFFL);
    }

    public void shiftFront(final int p_writtenBytes) {
        m_posFront += p_writtenBytes;
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
    void pushMessage(final AbstractMessage p_message, final int p_messageSize) throws NetworkException {
        int posBackUnlimited;
        int posBack;
        int posBackRelative;
        int posFront;
        int posFrontRelative;

        m_producers.incrementAndGet();
        while (m_consumerWaits) {
            // Consumer wants to flush
            m_producers.decrementAndGet();
            while (m_consumerWaits) {
                try {
                    synchronized (m_producers) {
                        m_producers.wait();
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
            posBackRelative = posBack % m_bufferSize; // posBackRelative must not exceed buffer
            posFront = (int) (m_posFront & 0x7FFFFFFFL);
            posFrontRelative = posFront % m_bufferSize;

            if (posBack == posFront /* empty */ ||
                    posBackRelative < posFrontRelative && posFrontRelative - posBackRelative > p_messageSize /* with overflow */ ||
                    posBackRelative > posFrontRelative && m_bufferSize - posBackRelative + posFrontRelative > p_messageSize /* without overflow */) {

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
                    synchronized (m_producers) {
                        m_producers.wait();
                    }
                } catch (InterruptedException ignore) {
                }
                m_producers.incrementAndGet();
            }
        }

        // Serialize message into ring buffer
        serialize(p_message, posBackRelative, p_messageSize, p_messageSize > m_bufferSize - posBackRelative /* with overflow */);

        // Leave
        m_producers.decrementAndGet();
    }

    protected abstract void serialize(final AbstractMessage p_message, final int p_start, final int p_messageSize, final boolean p_hasOverflow)
            throws NetworkException;

    // empty if both back and front are equal
    protected long popFrontShift() {
        int posBack;
        int posBackRelative;
        int posFront;
        int posFrontRelative;

        // Deny access to incoming producers
        m_consumerWaits = true;

        // Wait for all producers to finish copying
        while (m_producers.get() > 0) {
            Thread.yield();
        }

        posBack = (int) (m_posBack.get() & 0x7FFFFFFFL);
        posBackRelative = posBack % m_bufferSize;
        posFront = (int) (m_posFront & 0x7FFFFFFFL);
        posFrontRelative = posFront % m_bufferSize;

        if (posBackRelative < posFrontRelative) {
            posBackRelative = m_bufferSize;
        }

        m_consumerWaits = false;
        // TODO: expensive to enter synchronized block
        synchronized (m_producers) {
            m_producers.notifyAll();
        }

        return (long) posBackRelative << 32 | (long) posFrontRelative;
    }
}
