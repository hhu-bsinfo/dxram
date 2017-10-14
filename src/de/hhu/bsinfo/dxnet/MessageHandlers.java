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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.MessageHeader;
import de.hhu.bsinfo.dxnet.core.MessageHeaderPool;

/**
 * Provides message handlers for incoming messages
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 12.06.2017
 */
public final class MessageHandlers {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MessageHandlers.class.getSimpleName());

    private static final int POOL_SIZE = 25;

    private final DefaultMessageHandlerPool m_defaultMessageHandlerPool;
    private final ExclusiveMessageHandler m_exclusiveMessageHandler;

    private MessageHeader[] m_defaultMessageHeaders;
    private MessageHeader[] m_exclusiveMessageHeaders;
    private int m_numberOfDefaultMessageHeaders;
    private int m_numberOfExclusiveMessageHeaders;

    /**
     * Constructor
     *
     * @param p_numMessageHandlerThreads
     *         Number of message handler threads to run
     * @param p_messageReceivers
     *         Provides all registered message receivers
     */
    MessageHandlers(final int p_numMessageHandlerThreads, final boolean p_overprovisioning, final MessageReceiverStore p_messageReceivers,
            final MessageHeaderPool p_messageHeaderPool) {
        // default message handlers
        m_defaultMessageHandlerPool = new DefaultMessageHandlerPool(p_messageReceivers, p_messageHeaderPool, p_numMessageHandlerThreads, p_overprovisioning);

        // and one exclusive
        m_exclusiveMessageHandler = new ExclusiveMessageHandler(p_messageReceivers, p_messageHeaderPool, p_overprovisioning);

        m_defaultMessageHeaders = new MessageHeader[POOL_SIZE];
        m_exclusiveMessageHeaders = new MessageHeader[POOL_SIZE];
        m_numberOfDefaultMessageHeaders = 0;
        m_numberOfExclusiveMessageHeaders = 0;
    }

    /**
     * Return local pool capacity.
     *
     * @return the pool size
     */
    public static int getPoolSize() {
        return POOL_SIZE;
    }

    /**
     * Add a new message header. Pool locally first, write through when thread-local pool is full.
     *
     * @param p_header
     *         the message header
     */
    public boolean newHeader(final de.hhu.bsinfo.dxnet.core.MessageHeader p_header) {

        if (!p_header.isExclusive()) {
            m_defaultMessageHeaders[m_numberOfDefaultMessageHeaders] = p_header;
            if (++m_numberOfDefaultMessageHeaders == m_defaultMessageHeaders.length) {
                m_defaultMessageHandlerPool.newHeaders(m_defaultMessageHeaders, m_defaultMessageHeaders.length);
                m_numberOfDefaultMessageHeaders = 0;
                return true;
            }
        } else {
            m_exclusiveMessageHeaders[m_numberOfExclusiveMessageHeaders] = p_header;
            if (++m_numberOfExclusiveMessageHeaders == m_exclusiveMessageHeaders.length) {
                m_exclusiveMessageHandler.newHeaders(m_exclusiveMessageHeaders, m_exclusiveMessageHeaders.length);
                m_numberOfExclusiveMessageHeaders = 0;
                return true;
            }
        }
        return false;
    }

    /**
     * Write thread-locally pooled message headers to message header store.
     *
     * @return the number of pushed message headers
     */
    public int pushLeftHeaders() {
        int ret = 0;

        if (m_numberOfDefaultMessageHeaders > 0) {
            ret += m_numberOfDefaultMessageHeaders;
            m_defaultMessageHandlerPool.newHeaders(m_defaultMessageHeaders, m_numberOfDefaultMessageHeaders);
            m_numberOfDefaultMessageHeaders = 0;
        }

        if (m_numberOfExclusiveMessageHeaders > 0) {
            ret += m_numberOfExclusiveMessageHeaders;
            m_exclusiveMessageHandler.newHeaders(m_exclusiveMessageHeaders, m_numberOfExclusiveMessageHeaders);
            m_numberOfExclusiveMessageHeaders = 0;
        }

        return ret;
    }

    /**
     * Close the message handlers
     */
    void close() {
        // Shutdown default message handler(s)
        m_defaultMessageHandlerPool.shutdown();

        // Shutdown exclusive message handler
        m_exclusiveMessageHandler.shutdown();
    }
}
