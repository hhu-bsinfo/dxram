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

package de.hhu.bsinfo.dxnet;

import java.util.concurrent.atomic.AtomicInteger;

import de.hhu.bsinfo.dxnet.core.Message;

/**
 * Lock-free ring buffer for messages (single producer, multiple consumers)
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 01.05.2017
 */
public class MessageStore {
    private final int m_size;
    private final Message[] m_buffer;

    private final AtomicInteger m_posFront;
    private final AtomicInteger m_posBack;

    /**
     * Creates an instance of MessageStore
     *
     * @param p_size
     *         Must be a power of two to handle wrap around
     */
    MessageStore(final int p_size) {
        if (p_size % 2 != 0) {
            throw new IllegalStateException("Message store size must be a power of two, invalid value " + p_size);
        }

        m_size = p_size;
        m_buffer = new Message[m_size];

        m_posFront = new AtomicInteger(0);
        m_posBack = new AtomicInteger(0);
    }

    /**
     * Returns whether the ring-buffer is empty or not.
     *
     * @return whether the ring-buffer is empty or not
     */
    public boolean isEmpty() {
        return (m_posFront.get() & 0x7FFFFFFF) == (m_posBack.get() & 0x7FFFFFFF);
    }

    /**
     * Returns whether the ring-buffer is full or not.
     *
     * @return whether the ring-buffer is full or not
     */
    public boolean isFull() {
        return (m_posBack.get() + 1 & 0x7FFFFFFF) % m_size == (m_posFront.get() & 0x7FFFFFFF) % m_size;
    }

    /**
     * Returns the maximum number of pending buffers.
     *
     * @return the capacity
     */
    public int capacity() {
        return m_buffer.length;
    }

    /**
     * Adds a message at the end of the buffer.
     *
     * @param p_message
     *         the message
     * @return whether the job was added or not
     */
    boolean pushMessage(final Message p_message) {
        // & 0x7FFFFFFF to kill sign
        int posBack = m_posBack.get() & 0x7FFFFFFF;
        int posFront = m_posFront.get() & 0x7FFFFFFF;

        if ((posBack + 1 & 0x7FFFFFFF) % m_size == posFront % m_size) {
            // Return without adding the message if queue is full
            return false;
        }

        m_buffer[posBack % m_size] = p_message;
        m_posBack.incrementAndGet();

        return true;
    }

    /**
     * Gets a message from the beginning of the buffer.
     *
     * @return the message or null if empty
     */
    Message popMessage() {
        Message ret;

        while (true) {
            // & 0x7FFFFFFF to kill sign
            int posBack = m_posBack.get() & 0x7FFFFFFF;
            int posFront = m_posFront.get() & 0x7FFFFFFF;

            if (posFront == posBack) {
                // Ring-buffer is empty.
                return null;
            }

            ret = m_buffer[posFront % m_size];
            if (m_posFront.compareAndSet(posFront, posFront + 1 & 0x7FFFFFFF)) {
                break;
            }
        }

        return ret;
    }
}
