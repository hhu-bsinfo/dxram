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
 * Distributes incoming exclusive messages
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 30.08.2017
 */
final class ExclusiveMessageHandler {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ExclusiveMessageHandler.class.getSimpleName());
    private static final StatisticsOperation SOP_PUSH = StatisticsRecorderManager.getOperation(ExclusiveMessageHandler.class, "Push");
    private static final StatisticsOperation SOP_WAIT = StatisticsRecorderManager.getOperation(ExclusiveMessageHandler.class, "Wait");

    // must be a power of two to work with wrap around
    private static final int EXCLUSIVE_MESSAGE_STORE_SIZE = 32;

    private final MessageHeaderStore m_exclusiveMessageHeaders;

    private final MessageHandler m_exclusiveMessageHandler;

    /**
     * Creates an instance of ExlusiveMessageHandler
     *
     * @param p_messageReceivers
     *         Provides all registered message receivers
     */
    ExclusiveMessageHandler(final MessageReceiverStore p_messageReceivers, final MessageHeaderPool p_messageHeaderPool) {
        m_exclusiveMessageHeaders = new MessageHeaderStore(EXCLUSIVE_MESSAGE_STORE_SIZE);

        // #if LOGGER >= INFO
        LOGGER.info("Network: ExclusiveMessageHandler: Initialising thread");
        // #endif /* LOGGER >= INFO */

        m_exclusiveMessageHandler = new MessageHandler(p_messageReceivers, m_exclusiveMessageHeaders, p_messageHeaderPool);
        m_exclusiveMessageHandler.setName("Network: ExclusiveMessageHandler");
        m_exclusiveMessageHandler.start();
    }

    /**
     * Closes the exclusive message handler
     */
    void shutdown() {
        m_exclusiveMessageHandler.shutdown();
        LockSupport.unpark(m_exclusiveMessageHandler);
        m_exclusiveMessageHandler.interrupt();

        try {
            m_exclusiveMessageHandler.join();
            // #if LOGGER >= INFO
            LOGGER.info("Shutdown of ExclusiveMessageHandler successful");
            // #endif /* LOGGER >= INFO */
        } catch (final InterruptedException e) {
            // #if LOGGER >= WARN
            LOGGER.warn("Could not wait for exclusive message handler to finish. Interrupted");
            // #endif /* LOGGER >= WARN */
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
            while (!m_exclusiveMessageHeaders.pushMessageHeader(p_header)) {
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

        if (!m_exclusiveMessageHeaders.pushMessageHeaders(p_headers, p_messages)) {
            for (int i = 0; i < p_headers.length; i++) {
                if (p_headers[i] == null) {
                    break;
                }

                // Ignore network test messages (e.g. ping after response delay)
                if (!(p_headers[i].getType() == Messages.DEFAULT_MESSAGES_TYPE && p_headers[i].getSubtype() == Messages.SUBTYPE_DEFAULT_MESSAGE)) {
                    while (!m_exclusiveMessageHeaders.pushMessageHeader(p_headers[i])) {
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
        return m_exclusiveMessageHeaders.isEmpty();
    }
}
