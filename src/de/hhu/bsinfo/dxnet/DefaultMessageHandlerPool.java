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

import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.MessageHeader;
import de.hhu.bsinfo.dxnet.core.MessageHeaderPool;
import de.hhu.bsinfo.dxnet.core.messages.Messages;
import de.hhu.bsinfo.utils.stats.StatisticsOperation;
import de.hhu.bsinfo.utils.stats.StatisticsRecorderManager;

/**
 * Distributes incoming default messages
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 19.07.2016
 */
final class DefaultMessageHandlerPool {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DefaultMessageHandlerPool.class.getSimpleName());
    private static final StatisticsOperation SOP_PUSH = StatisticsRecorderManager.getOperation(DefaultMessageHandlerPool.class, "Push");
    private static final StatisticsOperation SOP_WAIT = StatisticsRecorderManager.getOperation(DefaultMessageHandlerPool.class, "Wait");

    // must be a power of two to work with wrap around
    private static final int SIZE_MESSAGE_STORE = 16 * 1024;

    private final MessageHeaderStore m_defaultMessageHeaders;

    private final MessageHandler[] m_threads;

    /**
     * Creates an instance of DefaultMessageHandlerPool
     *
     * @param p_numMessageHandlerThreads
     *         the number of default message handler
     */
    DefaultMessageHandlerPool(final MessageReceiverStore p_messageReceivers, final MessageHeaderPool p_messageHeaderPool,
            final int p_numMessageHandlerThreads) {
        m_defaultMessageHeaders = new MessageHeaderStore(SIZE_MESSAGE_STORE);

        // #if LOGGER >= INFO
        LOGGER.info("Network: DefaultMessageHandlerPool: Initialising %d threads", p_numMessageHandlerThreads);
        // #endif /* LOGGER >= INFO */

        MessageHandler t;
        m_threads = new MessageHandler[p_numMessageHandlerThreads];
        for (int i = 0; i < m_threads.length; i++) {
            t = new MessageHandler(p_messageReceivers, m_defaultMessageHeaders, p_messageHeaderPool);
            t.setName("Network: MessageHandler " + (i + 1));
            m_threads[i] = t;
            t.start();
        }
    }

    /**
     * Closes all default message handler
     */
    void shutdown() {
        MessageHandler t;
        for (int i = 0; i < m_threads.length; i++) {
            t = m_threads[i];
            t.shutdown();
            LockSupport.unpark(t);
            t.interrupt();

            try {
                t.join();
                // #if LOGGER >= INFO
                LOGGER.info("Shutdown of MessageHandler %d successful", i + 1);
                // #endif /* LOGGER >= INFO */
            } catch (final InterruptedException e) {
                // #if LOGGER >= WARN
                LOGGER.warn("Could not wait for default message handler to finish. Interrupted");
                // #endif /* LOGGER >= WARN */
            }
        }
    }

    /**
     * Enqueue a message header
     *
     * @param p_header
     *         the message header
     */
    void newHeader(final MessageHeader p_header) {
        // #ifdef STATISTICS
        // SOP_PUSH.enter();
        // #endif /* STATISTICS */

        // Ignore network test messages (e.g. ping after response delay)
        if (!(p_header.getType() == Messages.DEFAULT_MESSAGES_TYPE && p_header.getSubtype() == Messages.SUBTYPE_DEFAULT_MESSAGE)) {
            while (!m_defaultMessageHeaders.pushMessageHeader(p_header)) {
                // #ifdef STATISTICS
                // SOP_WAIT.enter();
                // #endif /* STATISTICS */

                LockSupport.parkNanos(100);

                // #ifdef STATISTICS
                // SOP_WAIT.leave();
                // #endif /* STATISTICS */
            }
        }

        // #ifdef STATISTICS
        // SOP_PUSH.leave();
        // #endif /* STATISTICS */
    }

    void newHeaders(final MessageHeader[] p_headers, final int p_messages) {
        // #ifdef STATISTICS
        // SOP_PUSH.enter();
        // #endif /* STATISTICS */

        if (!m_defaultMessageHeaders.pushMessageHeaders(p_headers, p_messages)) {
            for (int i = 0; i < p_messages; i++) {
                // Ignore network test messages (e.g. ping after response delay)
                if (!(p_headers[i].getType() == Messages.DEFAULT_MESSAGES_TYPE && p_headers[i].getSubtype() == Messages.SUBTYPE_DEFAULT_MESSAGE)) {
                    while (!m_defaultMessageHeaders.pushMessageHeader(p_headers[i])) {
                        // #ifdef STATISTICS
                        // SOP_WAIT.enter();
                        // #endif /* STATISTICS */

                        LockSupport.parkNanos(100);

                        // #ifdef STATISTICS
                        // SOP_WAIT.leave();
                        // #endif /* STATISTICS */
                    }
                }
            }
        }

        // #ifdef STATISTICS
        // SOP_PUSH.leave();
        // #endif /* STATISTICS */
    }

    boolean isEmpty() {
        return m_defaultMessageHeaders.isEmpty();
    }
}
