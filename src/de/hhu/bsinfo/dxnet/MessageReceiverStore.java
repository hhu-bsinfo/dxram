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

import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Storage that holds all registered message receivers
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 12.06.2017
 */
class MessageReceiverStore {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MessageHandlers.class.getSimpleName());

    private MessageReceiver[][] m_receivers;
    private final ReentrantLock m_receiversLock;

    private final int m_requestTimeOut;

    /**
     * Constructor
     *
     * @param p_requestTimeOut
     *         Timeout in ms for requests
     */
    MessageReceiverStore(final int p_requestTimeOut) {
        m_requestTimeOut = p_requestTimeOut;

        m_receivers = new MessageReceiver[100][100];
        m_receiversLock = new ReentrantLock(false);
    }

    /**
     * Get a receiver
     *
     * @param p_type
     *         Type of the message
     * @param p_subtype
     *         Subtype of the message
     * @return Receiver for the specified type/subtype messages
     */
    MessageReceiver getReceiver(final byte p_type, final byte p_subtype) {
        long deadline;
        MessageReceiver messageReceiver = m_receivers[p_type][p_subtype];

        // Try again in a loop, if receivers were not registered. Stop if request timeout is reached as answering later has no effect
        if (messageReceiver == null) {
            // #if LOGGER >= WARN
            LOGGER.warn("Message receiver null for %d, %d! Waiting...", p_type, p_subtype);
            // #endif /* LOGGER >= WARN */
            deadline = System.currentTimeMillis() + m_requestTimeOut;
            while (messageReceiver == null && System.currentTimeMillis() < deadline) {
                m_receiversLock.lock();
                messageReceiver = m_receivers[p_type][p_subtype];
                m_receiversLock.unlock();
            }
        }

        return messageReceiver;
    }

    /**
     * Registers a message receiver
     *
     * @param p_type
     *         the message type
     * @param p_subtype
     *         the message subtype
     * @param p_receiver
     *         the receiver
     */
    void register(final byte p_type, final byte p_subtype, final MessageReceiver p_receiver) {
        if (p_receiver != null) {
            m_receiversLock.lock();
            // enlarge array
            if (m_receivers.length <= p_type) {
                final MessageReceiver[][] newArray = new MessageReceiver[p_type + 1][];
                System.arraycopy(m_receivers, 0, newArray, 0, m_receivers.length);
                m_receivers = newArray;
            }

            // create new sub array when it is not existing until now
            if (m_receivers[p_type] == null) {
                m_receivers[p_type] = new MessageReceiver[p_subtype + 1];
            }

            // enlarge subtype array
            if (m_receivers[p_type].length <= p_subtype) {
                final MessageReceiver[] newArray = new MessageReceiver[p_subtype + 1];
                System.arraycopy(m_receivers[p_type], 0, newArray, 0, m_receivers[p_type].length);
                m_receivers[p_type] = newArray;
            }

            if (m_receivers[p_type][p_subtype] != null) {
                // #if LOGGER >= WARN
                LOGGER.warn("Receiver for %d %d is already registered", p_type, p_subtype);
                // #endif /* LOGGER >= WARN */
            }
            m_receivers[p_type][p_subtype] = p_receiver;

            // #if LOGGER >= TRACE
            LOGGER.trace("Added new MessageReceiver %s for %d %d", p_receiver.getClass(), p_type, p_subtype);
            // #endif /* LOGGER >= TRACE */
            m_receiversLock.unlock();
        }
    }

    /**
     * Unregisters a message receiver
     *
     * @param p_type
     *         the message type
     * @param p_subtype
     *         the message subtype
     * @param p_receiver
     *         the receiver
     */
    void unregister(final byte p_type, final byte p_subtype, final MessageReceiver p_receiver) {
        if (p_receiver != null) {
            m_receiversLock.lock();
            m_receivers[p_type][p_subtype] = null;
            m_receiversLock.unlock();
        }
    }
}
