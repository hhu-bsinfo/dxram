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

/**
 * Pools message headers for AbstractPipeIns (multiple producers, single consumer; buffer is filled at the beginning)
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 05.10.2017
 */
public final class MessageHeaderPool {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MessageHeaderPool.class.getSimpleName());

    private final int m_size;
    private final MessageHeader[] m_buffer;

    private volatile int m_posFront;
    private AtomicInteger m_posBackProducer;
    private AtomicInteger m_posBackConsumer;

    /**
     * Creates an instance of MessageHeaderPool
     *
     * @param p_size
     *         the pool size
     */
    public MessageHeaderPool(final int p_size) {
        if ((p_size & p_size - 1) != 0) {
            throw new NetworkRuntimeException("MessageHeader pool size must be a power of 2!");
        }

        m_size = p_size;
        m_buffer = new MessageHeader[m_size];
        for (int i = 0; i < m_size; i++) {
            m_buffer[i] = new MessageHeader();
        }

        m_posFront = 0;
        m_posBackProducer = new AtomicInteger(0);
        m_posBackConsumer = new AtomicInteger(0);
    }

    /**
     * Get a batch of pooled message headers
     *
     * @return empty message headers
     */
    boolean getHeaders(final MessageHeader[] p_messageHeaders) {
        // & 0x7FFFFFFF to kill sign
        int posFront = m_posFront & 0x7FFFFFFF;
        int posBack = m_posBackConsumer.get();

        if ((posBack + m_size & 0x7FFFFFFF) >= (posFront + p_messageHeaders.length & 0x7FFFFFFF) ||
                /* 31-bit overflow in posBack but not posFront */
                (posBack + m_size & 0x7FFFFFFF) < (posBack & 0x7FFFFFFF) && (posFront + p_messageHeaders.length & 0x7FFFFFFF) > (posBack & 0x7FFFFFFF)) {
            for (int i = 0; i < p_messageHeaders.length; i++) {
                p_messageHeaders[i] = m_buffer[(posFront + i & 0x7FFFFFFF) % m_size];
            }
            m_posFront += p_messageHeaders.length;

            return true;
        }

        // Ring-buffer is empty.

        // #if LOGGER >= WARN
        LOGGER.warn("Insufficient pooled message headers. Allocating temporary message header.");
        // #endif /* LOGGER >= WARN *//*

        return false;
    }

    /**
     * Get a pooled message header
     *
     * @return an empty message header
     */
    public MessageHeader getHeader() {
        MessageHeader ret;

        // & 0x7FFFFFFF to kill sign
        int posFront = m_posFront & 0x7FFFFFFF;

        if ((m_posBackConsumer.get() + m_size & 0x7FFFFFFF) != posFront) {
            ret = m_buffer[posFront % m_size];
            m_posFront++;

            return ret;
        }

        // Ring-buffer is empty.

        // #if LOGGER >= WARN
        LOGGER.warn("Insufficient pooled message headers. Allocating temporary message header.");
        // #endif /* LOGGER >= WARN *//*

        return new MessageHeader();
    }

    /**
     * Return a batch of message headers
     *
     * @param p_messageHeaders
     *         the message headers
     */
    void returnHeaders(final MessageHeader[] p_messageHeaders) {
        while (true) {
            // & 0x7FFFFFFF to kill sign
            int posFront = m_posFront & 0x7FFFFFFF;
            int posBackSigned = m_posBackProducer.get();
            int posBack = posBackSigned & 0x7FFFFFFF;
            if ((posBack + p_messageHeaders.length & 0x7FFFFFFF) > posFront) {
                // Cannot return all message headers at once -> try returning single message headers until pool is full
                for (int i = 0; i < p_messageHeaders.length; i++) {
                    if (!returnHeader(p_messageHeaders[i])) {
                        break;
                    }
                }

                return;
            }

            if (m_posBackProducer.compareAndSet(posBackSigned, posBackSigned + p_messageHeaders.length)) {
                for (int i = 0; i < p_messageHeaders.length; i++) {
                    m_buffer[(posBack + i & 0x7FFFFFFF) % m_size] = p_messageHeaders[i];
                }

                // First atomic is necessary to synchronize producers, second to inform consumer after message header has been added
                while (!m_posBackConsumer.compareAndSet(posBackSigned, posBackSigned + p_messageHeaders.length)) {
                    // Producer needs to wait for all other submissions prior to this one
                    // (this thread overtook at least one other producer since updating posBackProducer)
                    Thread.yield();
                }
                break;
            }
        }
    }

    /**
     * Return a message header
     *
     * @param p_messageHeader
     *         the message header
     * @return whether returning was successful
     */
    private boolean returnHeader(final MessageHeader p_messageHeader) {
        while (true) {
            // & 0x7FFFFFFF to kill sign
            int posFront = m_posFront & 0x7FFFFFFF;
            int posBackSigned = m_posBackProducer.get();
            int posBack = posBackSigned & 0x7FFFFFFF;
            if (posBack == posFront) {
                // Return without adding the message header if queue is full (happens if message headers were created in getHeader())

                // #if LOGGER >= WARN
                LOGGER.warn("Cannot add message headers. Buffer is full.");
                // #endif /* LOGGER >= WARN *//*

                return false;
            }

            if (m_posBackProducer.compareAndSet(posBackSigned, posBackSigned + 1)) {
                m_buffer[posBack % m_size] = p_messageHeader;

                // First atomic is necessary to synchronize producers, second to inform consumer after message header has been added
                while (!m_posBackConsumer.compareAndSet(posBackSigned, posBackSigned + 1)) {
                    // Producer needs to wait for all other submissions prior to this one
                    // (this thread overtook at least one other producer since updating posBackProducer)
                    Thread.yield();
                }
                break;
            }
        }
        return true;
    }
}
