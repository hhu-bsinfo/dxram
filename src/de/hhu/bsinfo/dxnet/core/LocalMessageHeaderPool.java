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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Pools message headers for AbstractPipeIns (multiple producers, single consumer; buffer is filled at the beginning)
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 05.10.2017
 */
public class LocalMessageHeaderPool {
    private static final Logger LOGGER = LogManager.getFormatterLogger(LocalMessageHeaderPool.class.getSimpleName());

    private static final int SIZE = 25;

    private final MessageHeader[] m_gets;
    private int m_posGets;

    private final MessageHeader[] m_returns;
    private int m_posReturns;

    private final MessageHeaderPool m_globalPool;
    private final int m_size;

    /**
     * Creates an instance of LocalMessageHeaderPool
     *
     * @param p_headerPool
     *         the global message header pool
     */
    public LocalMessageHeaderPool(final MessageHeaderPool p_headerPool) {
        m_globalPool = p_headerPool;
        // Add thread ID to reduce collision probability
        m_size = (int) (SIZE + Thread.currentThread().getId());

        m_gets = new MessageHeader[m_size];
        m_globalPool.getHeaders(m_gets);
        m_posGets = 0;

        m_returns = new MessageHeader[m_size];
        m_posReturns = 0;
    }

    /**
     * Get a pooled message header
     *
     * @return an empty message header
     */
    public MessageHeader getHeader() {
        if (m_posGets == m_size) {
            // Get headers from global pool
            if (m_globalPool.getHeaders(m_gets)) {
                m_posGets = 0;
            } else {
                // Not enough headers available -> get one
                return m_globalPool.getHeader();
            }
        }
        return m_gets[m_posGets++];
    }

    /**
     * Returns the message header to pool
     *
     * @param p_messageHeader
     *         the message header
     */
    void returnHeader(final MessageHeader p_messageHeader) {
        p_messageHeader.clear();

        if (m_posReturns == m_size) {
            // Return headers to global pool
            m_globalPool.returnHeaders(m_returns);
            m_posReturns = 0;
        }
        m_returns[m_posReturns++] = p_messageHeader;
    }
}
