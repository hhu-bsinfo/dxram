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

import de.hhu.bsinfo.dxnet.core.MessageHeader;

/**
 * Lock-free ring buffer for message headers (single producer, multiple consumers)
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 01.05.2017
 */
public class MessageHeaderStore {
    private final int m_size;
    private final MessageHeader[] m_buffer;

    private final AtomicInteger m_posBack;
    private volatile int m_posFront;

    /**
     * Creates an instance of MessageHeaderStore
     *
     * @param p_size
     *         Must be a power of two to handle wrap around
     */
    MessageHeaderStore(final int p_size) {
        if (p_size % 2 != 0) {
            throw new IllegalStateException("Message store size must be a multiple of two, invalid value " + p_size);
        }

        m_size = p_size;
        m_buffer = new MessageHeader[m_size];

        m_posBack = new AtomicInteger(0);
        m_posFront = 0;
    }

    /**
     * Returns whether the ring-buffer is empty or not.
     *
     * @return whether the ring-buffer is empty or not
     */
    public boolean isEmpty() {
        return (m_posBack.get() & 0x7FFFFFFF) == (m_posFront & 0x7FFFFFFF);
    }

    /**
     * Adds a message header.
     *
     * @param p_header
     *         the message
     * @return whether the message header was added or not
     */
    boolean pushMessageHeader(final MessageHeader p_header) {
        // & 0x7FFFFFFF to kill sign
        int posFront = m_posFront & 0x7FFFFFFF;

        if ((m_posBack.get() + m_size & 0x7FFFFFFF) != posFront) {
            m_buffer[posFront % m_size] = p_header;
            m_posFront++;

            return true;
        }

        // Return without adding the message header if queue is full
        return false;
    }

    /**
     * Adds a batch of message headers.
     *
     * @param p_headers
     *         the message headers to be processed
     * @param p_messages
     *         the number of valid entries in the array
     * @return whether the message headers were added or not
     */
    boolean pushMessageHeaders(final MessageHeader[] p_headers, final int p_messages) {
        // & 0x7FFFFFFF to kill sign
        int posFront = m_posFront & 0x7FFFFFFF;
        int posBack = m_posBack.get();

        if ((posBack + m_size & 0x7FFFFFFF) >= (posFront + p_messages & 0x7FFFFFFF) ||
                /* 31-bit overflow in posBack but not posFront */
                (posBack + m_size & 0x7FFFFFFF) < (posBack & 0x7FFFFFFF) && (posFront + p_messages & 0x7FFFFFFF) > (posFront & 0x7FFFFFFF)) {

            for (int i = 0; i < p_messages; i++) {
                m_buffer[(posFront + i & 0x7FFFFFFF) % m_size] = p_headers[i];
            }
            m_posFront += p_messages;

            return true;
        }

        // Return without adding the message headers if not all message header fit
        return false;
    }

    /**
     * Gets a message header from the beginning of the buffer.
     *
     * @return the message or null if empty
     */
    MessageHeader popMessageHeader() {
        MessageHeader ret;

        while (true) {
            // Get back before front, otherwise back can overtake front when scheduler interrupts thread between get calls
            int posBackSigned = m_posBack.get();
            int posBack = posBackSigned & 0x7FFFFFFF;
            if (posBack == (m_posFront & 0x7FFFFFFF)) {
                // Ring-buffer is empty.
                ret = null;
                break;
            }

            ret = m_buffer[posBack % m_size];

            if (m_posBack.compareAndSet(posBackSigned, posBackSigned + 1)) {
                break;
            }
        }

        return ret;
    }
}
