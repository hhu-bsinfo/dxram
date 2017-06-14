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

package de.hhu.bsinfo.ethnet;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongArray;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.ethnet.core.AbstractConnection;
import de.hhu.bsinfo.ethnet.core.AbstractConnectionManager;
import de.hhu.bsinfo.ethnet.core.AbstractMessage;
import de.hhu.bsinfo.ethnet.core.AbstractRequest;
import de.hhu.bsinfo.ethnet.core.ConnectionManagerListener;
import de.hhu.bsinfo.ethnet.core.MessageCreator;
import de.hhu.bsinfo.ethnet.core.MessageDirectory;
import de.hhu.bsinfo.ethnet.core.NetworkException;
import de.hhu.bsinfo.ethnet.core.RequestMap;
import de.hhu.bsinfo.ethnet.nio.NIOConnectionManager;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Access the network through Java NIO
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 * @author Marc Ewert, marc.ewert@hhu.de, 14.08.2014
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.11.2015
 */
public final class NetworkHandler {
    private static final Logger LOGGER = LogManager.getFormatterLogger(NetworkHandler.class.getSimpleName());

    private final short m_ownNodeId;

    private final MessageHandlers m_messageHandlers;
    private final MessageDirectory m_messageDirectory;
    private final MessageCreator m_messageCreator;

    private final AtomicLongArray m_lastFailures;

    private final RequestMap m_requestMap;
    private final int m_timeOut;

    private final AbstractConnectionManager m_connectionManager;

    // TODO doc
    public NetworkHandler(final short p_ownNodeId, final int p_maxConnections, final int p_bufferSize, final int p_flowControlWindowSize,
            final int p_numMessageHandlerThreads, final int p_requestMapSize, final int p_requestTimeOut, final int p_connectionTimeout,
            final NodeMap p_nodeMap, final boolean p_infiniband) {
        m_ownNodeId = p_ownNodeId;

        m_messageHandlers = new MessageHandlers(p_numMessageHandlerThreads, p_requestTimeOut);
        m_messageDirectory = new MessageDirectory(p_requestTimeOut);

        // #if LOGGER >= INFO
        LOGGER.info("Network: MessageCreator");
        // #endif /* LOGGER >= INFO */
        m_messageCreator = new MessageCreator(p_bufferSize);
        m_messageCreator.setName("Network: MessageCreator");
        m_messageCreator.start();

        m_lastFailures = new AtomicLongArray(65536);

        m_requestMap = new RequestMap(p_requestMapSize);
        m_timeOut = p_requestTimeOut;

        if (!p_infiniband) {
            m_connectionManager = new NIOConnectionManager(p_ownNodeId, p_maxConnections, p_bufferSize, p_flowControlWindowSize, p_connectionTimeout, p_nodeMap,
                    m_messageDirectory, m_requestMap, m_messageCreator, m_messageHandlers);
        } else {
            // TODO infiniband
            throw new NotImplementedException();
        }
    }

    /**
     * Returns the status of the network module
     *
     * @return the status
     */
    public String getStatus() {
        String str = "";

        str += m_connectionManager.getConnectionStatuses();

        return str;
    }

    public void setConnectionManagerListener(final ConnectionManagerListener p_listener) {
        m_connectionManager.setListener(p_listener);
    }

    /**
     * Registers a message type
     *
     * @param p_type
     *         the unique type
     * @param p_subtype
     *         the unique subtype
     * @param p_class
     *         the calling class
     */
    public void registerMessageType(final byte p_type, final byte p_subtype, final Class<?> p_class) {
        boolean ret;

        ret = m_messageDirectory.register(p_type, p_subtype, p_class);

        // #if LOGGER >= WARN
        if (!ret) {
            LOGGER.warn("Registering network message %s for type %s and subtype %s failed, type and subtype already used", p_class.getSimpleName(), p_type,
                    p_subtype);
        }
        // #endif /* LOGGER >= WARN */
    }

    /**
     * Closes the network handler
     */
    public void close() {
        m_messageCreator.shutdown();

        m_messageHandlers.close();
        m_connectionManager.close();
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
    public void register(final byte p_type, final byte p_subtype, final MessageReceiver p_receiver) {
        m_messageHandlers.register(p_type, p_subtype, p_receiver);
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
    public void unregister(final byte p_type, final byte p_subtype, final MessageReceiver p_receiver) {
        m_messageHandlers.unregister(p_type, p_subtype, p_receiver);
    }

    /**
     * Connects a node.
     *
     * @param p_nodeID
     *         Node to connect
     * @throws NetworkException
     *         If sending the message failed
     */
    public void connectNode(final short p_nodeID) throws NetworkException {
        // #if LOGGER == TRACE
        LOGGER.trace("Entering connectNode with: p_nodeID=0x%X", p_nodeID);
        // #endif /* LOGGER == TRACE */

        try {
            if (m_connectionManager.getConnection(p_nodeID) == null) {
                throw new IOException("Connection to " + NodeID.toHexString(p_nodeID) + " could not be established");
            }
        } catch (final IOException e) {
            // #if LOGGER >= DEBUG
            LOGGER.debug("IOException during connection lookup", e);
            // #endif /* LOGGER >= DEBUG */
            throw new NetworkDestinationUnreachableException(p_nodeID);
        }

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting connectNode");
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Sends a message
     *
     * @param p_message
     *         the message to send
     * @throws NetworkException
     *         If sending the message failed
     */
    public void sendMessage(final AbstractMessage p_message) throws NetworkException {
        AbstractConnection connection;

        // #if LOGGER == TRACE
        LOGGER.trace("Entering sendMessage with: p_message=%s", p_message);
        // #endif /* LOGGER == TRACE */

        if (p_message.getDestination() == m_ownNodeId) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid destination 0x%X. No loopback allowed.", p_message.getDestination());
            // #endif /* LOGGER >= ERROR */
        } else {
            try {
                connection = m_connectionManager.getConnection(p_message.getDestination());
            } catch (final NetworkException e) {
                // #if LOGGER >= DEBUG
                LOGGER.debug("Connection invalid. Ignoring connection exceptions regarding 0x%X during the next second!", p_message.getDestination());
                // #endif /* LOGGER >= DEBUG */
                throw new NetworkDestinationUnreachableException(p_message.getDestination());
            }
            try {
                if (connection != null) {
                    connection.postMessage(p_message);
                } else {
                    long timestamp = m_lastFailures.get(p_message.getDestination() & 0xFFFF);
                    if (timestamp == 0 || timestamp + 1000 < System.currentTimeMillis()) {
                        m_lastFailures.set(p_message.getDestination() & 0xFFFF, System.currentTimeMillis());

                        // #if LOGGER >= DEBUG
                        LOGGER.debug("Connection invalid. Ignoring connection exceptions regarding 0x%X during the next second!", p_message.getDestination());
                        // #endif /* LOGGER >= DEBUG */
                        throw new NetworkDestinationUnreachableException(p_message.getDestination());
                    } else {
                        return;
                    }
                }
            } catch (final NetworkException e) {
                // #if LOGGER >= DEBUG
                LOGGER.debug("Sending data failed ", e);
                // #endif /* LOGGER >= DEBUG */
                throw new NetworkException("Sending data failed ", e);
            }
        }

        // #if LOGGER == TRACE
        LOGGER.trace("Exiting sendMessage");
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Send the Request and wait for fulfillment (wait for response).
     *
     * @param p_request
     *         The request to send.
     * @param p_timeout
     *         The amount of time to wait for a response
     * @param p_waitForResponses
     *         Set to false to not wait/block until the response arrived
     * @throws NetworkException
     *         If sending the message failed
     */
    public void sendSync(final AbstractRequest p_request, final int p_timeout, final boolean p_waitForResponses) throws NetworkException {
        // #if LOGGER == TRACE
        LOGGER.trace("Sending request (sync): %s", p_request);
        // #endif /* LOGGER == TRACE */

        try {
            m_requestMap.put(p_request);
            sendMessage(p_request);
        } catch (final NetworkException e) {
            m_requestMap.remove(p_request.getRequestID());
            throw e;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("Waiting for response to request: %s", p_request);
        // #endif /* LOGGER == TRACE */

        int timeout = p_timeout != -1 ? p_timeout : m_timeOut;
        try {
            if (p_waitForResponses) {
                p_request.waitForResponse(timeout);
            }
        } catch (final NetworkResponseTimeoutException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending sync, waiting for responses %s failed, timeout", p_request);
            // #endif /* LOGGER >= ERROR */

            m_requestMap.remove(p_request.getRequestID());

            throw e;
        }
    }

    /**
     * Cancel all pending requests waiting for a response
     *
     * @param p_nodeId
     *         Node id of the target node the requests
     *         are waiting for a response
     */
    public void cancelAllRequests(final short p_nodeId) {
        m_requestMap.removeAll(p_nodeId);
    }
}
