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

import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.AbstractConnection;
import de.hhu.bsinfo.dxnet.core.AbstractConnectionManager;
import de.hhu.bsinfo.dxnet.core.CoreConfig;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.MessageCreator;
import de.hhu.bsinfo.dxnet.core.MessageDirectory;
import de.hhu.bsinfo.dxnet.core.MessageHeaderPool;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxnet.core.RequestMap;
import de.hhu.bsinfo.dxnet.core.messages.DefaultMessage;
import de.hhu.bsinfo.dxnet.core.messages.Messages;
import de.hhu.bsinfo.dxnet.ib.IBConfig;
import de.hhu.bsinfo.dxnet.ib.IBConnectionManager;
import de.hhu.bsinfo.dxnet.loopback.LoopbackConfig;
import de.hhu.bsinfo.dxnet.loopback.LoopbackConnectionManager;
import de.hhu.bsinfo.dxnet.nio.NIOConfig;
import de.hhu.bsinfo.dxnet.nio.NIOConnectionManager;
import de.hhu.bsinfo.utils.NodeID;
import de.hhu.bsinfo.utils.stats.StatisticsOperation;
import de.hhu.bsinfo.utils.stats.StatisticsRecorderManager;

/**
 * DXNet main class. The network subsystem supports different types of transport. Ethernet using Java NIO and InfiniBand using libibverbs through an
 * additional JNI library (libJNIIbdxnet). The network allows you to easily send messages, requests or responses to requests to another destination, receive
 * incoming messages or requests and process them in your application.
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 * @author Marc Ewert, marc.ewert@hhu.de, 14.08.2014
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.11.2015
 * @author Stefan Nothaas, kevin.beineke@hhu.de, 11.08.2017
 */
public final class DXNet {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXNet.class.getSimpleName());
    private static final StatisticsOperation SOP_SEND = StatisticsRecorderManager.getOperation(DXNet.class, "MessageSend");
    private static final StatisticsOperation SOP_SEND_SYNC = StatisticsRecorderManager.getOperation(DXNet.class, "MessageSendSync");
    private static final StatisticsOperation SOP_WAIT_RESPONSE = StatisticsRecorderManager.getOperation(DXNet.class, "WaitForResponse");
    private static final StatisticsOperation SOP_REQ_RESP_RTT = StatisticsRecorderManager.getOperation(DXNet.class, "ReqRespRTT");

    private static final int MESSAGE_HEADER_POOL_SIZE = 1000 * 1000;

    private final CoreConfig m_coreConfig;
    private final NIOConfig m_nioConfig;
    private final IBConfig m_ibConfig;
    private final LoopbackConfig m_loopbackConfig;

    private final MessageReceiverStore m_messageReceivers;
    private final MessageHandlers m_messageHandlers;
    private final MessageDirectory m_messageDirectory;
    private final MessageCreator m_messageCreator;

    private final AtomicLongArray m_lastFailures;

    private final RequestMap m_requestMap;
    private final int m_timeOut;

    private final AbstractConnectionManager m_connectionManager;

    /**
     * Constructor
     *
     * @param p_coreConfig
     *         Configuration parameters for core
     * @param p_nioConfig
     *         Configuration parameters for NIO
     * @param p_ibConfig
     *         Configuration parameters for InfiniBand
     * @param p_nodeMap
     *         NodeMap implementation to lookup node ids
     */
    public DXNet(final CoreConfig p_coreConfig, final NIOConfig p_nioConfig, final IBConfig p_ibConfig, final LoopbackConfig p_loopbackConfig,
            final NodeMap p_nodeMap) {
        m_coreConfig = p_coreConfig;
        m_nioConfig = p_nioConfig;
        m_ibConfig = p_ibConfig;
        m_loopbackConfig = p_loopbackConfig;

        if ("Ethernet".equals(m_coreConfig.getDevice())) {
            m_messageReceivers = new MessageReceiverStore((int) m_nioConfig.getRequestTimeOut().getMs());
        } else if ("Infiniband".equals(m_coreConfig.getDevice())) {
            m_messageReceivers = new MessageReceiverStore((int) m_ibConfig.getRequestTimeOut().getMs());
        } else {
            m_messageReceivers = new MessageReceiverStore((int) m_loopbackConfig.getRequestTimeOut().getMs());
        }

        MessageHeaderPool headerPool = new MessageHeaderPool(MESSAGE_HEADER_POOL_SIZE);
        m_messageHandlers = new MessageHandlers(m_coreConfig.getNumMessageHandlerThreads(), m_messageReceivers, headerPool);

        if ("Ethernet".equals(m_coreConfig.getDevice())) {
            m_messageDirectory = new MessageDirectory((int) m_nioConfig.getRequestTimeOut().getMs());
        } else if ("Infiniband".equals(m_coreConfig.getDevice())) {
            m_messageDirectory = new MessageDirectory((int) m_ibConfig.getRequestTimeOut().getMs());
        } else {
            m_messageDirectory = new MessageDirectory((int) m_loopbackConfig.getRequestTimeOut().getMs());
        }

        m_messageDirectory.register(Messages.DEFAULT_MESSAGES_TYPE, Messages.SUBTYPE_DEFAULT_MESSAGE, DefaultMessage.class);

        // #if LOGGER >= INFO
        LOGGER.info("Network: MessageCreator");
        // #endif /* LOGGER >= INFO */

        if ("Ethernet".equals(m_coreConfig.getDevice())) {
            m_messageCreator = new MessageCreator((int) m_nioConfig.getOugoingRingBufferSize().getBytes());
        } else if ("Infiniband".equals(m_coreConfig.getDevice())) {
            m_messageCreator = new MessageCreator((int) m_ibConfig.getOugoingRingBufferSize().getBytes());
        } else {
            m_messageCreator = new MessageCreator((int) m_loopbackConfig.getOugoingRingBufferSize().getBytes());
        }

        m_messageCreator.setName("Network: MessageCreator");
        m_messageCreator.start();

        m_lastFailures = new AtomicLongArray(65536);

        m_requestMap = new RequestMap(m_coreConfig.getRequestMapSize());
        m_timeOut = (int) m_nioConfig.getRequestTimeOut().getMs();

        if ("Ethernet".equals(m_coreConfig.getDevice())) {
            m_connectionManager =
                    new NIOConnectionManager(m_coreConfig, m_nioConfig, p_nodeMap, m_messageDirectory, m_requestMap, m_messageCreator.getIncomingBufferQueue(),
                            headerPool, m_messageHandlers);
        } else if ("Infiniband".equals(m_coreConfig.getDevice())) {
            m_connectionManager =
                    new IBConnectionManager(m_coreConfig, m_ibConfig, p_nodeMap, m_messageDirectory, m_requestMap, m_messageCreator.getIncomingBufferQueue(),
                            headerPool, m_messageHandlers);
            ((IBConnectionManager) m_connectionManager).init();
        } else {
            m_connectionManager = new LoopbackConnectionManager(m_coreConfig, m_loopbackConfig, p_nodeMap, m_messageDirectory, m_requestMap,
                    m_messageCreator.getIncomingBufferQueue(), headerPool, m_messageHandlers);
        }
    }

    /**
     * Get the status of the network system (debug string)
     */
    public String getStatus() {
        String str = "";

        str += m_connectionManager.getConnectionStatuses();

        return str;
    }

    /**
     * Set the ConnectionManager listener
     */
    public void setConnectionManagerListener(final ConnectionManagerListener p_listener) {
        m_connectionManager.setListener(p_listener);
    }

    /**
     * Registers a new message type
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
     * Closes the network
     */
    public void close() {
        m_messageHandlers.close();

        m_messageCreator.shutdown();

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
        m_messageReceivers.register(p_type, p_subtype, p_receiver);
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
        m_messageReceivers.unregister(p_type, p_subtype, p_receiver);
    }

    /**
     * Try to force connect to a specific node. When sending messages, the system tries to connect to the destination first, if it is not connected,
     * automatically. This call is mainly used to detect node failures.
     *
     * @param p_nodeID
     *         Node to connect to
     * @throws NetworkException
     *         If connecting to the node failed
     */
    public void connectNode(final short p_nodeID) throws NetworkException {
        // #if LOGGER == TRACE
        // LOGGER.trace("Entering connectNode with: p_nodeID=0x%X", p_nodeID);
        // #endif /* LOGGER == TRACE */

        try {
            if (m_connectionManager.getConnection(p_nodeID) == null) {
                throw new NetworkException("Connection to " + NodeID.toHexString(p_nodeID) + " could not be established");
            }
        } catch (final NetworkException e) {
            // #if LOGGER >= DEBUG
            LOGGER.debug("IOException during connection lookup", e);
            // #endif /* LOGGER >= DEBUG */
            throw new NetworkDestinationUnreachableException(p_nodeID);
        }

        // #if LOGGER == TRACE
        // LOGGER.trace("Exiting connectNode");
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
    public void sendMessage(final Message p_message) throws NetworkException {
        AbstractConnection connection;

        // #if LOGGER == TRACE
        // LOGGER.trace("Entering sendMessage with: p_message=%s", p_message);
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        // SOP_SEND.enter();
        // #endif /* STATISTICS */

        if (p_message.getDestination() == m_coreConfig.getOwnNodeId()) {
            // #if LOGGER >= ERROR
            LOGGER.error("Invalid destination 0x%X. No loopback allowed.", p_message.getDestination());
            // #endif /* LOGGER >= ERROR */
        } else {
            try {
                connection = m_connectionManager.getConnection(p_message.getDestination());
            } catch (final NetworkException e) {
                // #if LOGGER >= DEBUG
                LOGGER.debug("Connection to 0x%X could not be established!", p_message.getDestination());
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
                    }
                }
            } catch (final NetworkException e) {
                // #if LOGGER >= DEBUG
                LOGGER.debug("Sending data failed ", e);
                // #endif /* LOGGER >= DEBUG */
                throw new NetworkException("Sending data failed ", e);
            }
        }

        // #ifdef STATISTICS
        // SOP_SEND.leave();
        // #endif /* STATISTICS */

        // #if LOGGER == TRACE
        // LOGGER.trace("Exiting sendMessage");
        // #endif /* LOGGER == TRACE */
    }

    /**
     * Send request and wait for fulfillment (wait for response).
     *
     * @param p_request
     *         The request to send.
     * @param p_timeout
     *         The amount of time to wait for a response
     * @param p_waitForResponses
     *         Set to false to not wait/block until the response arrived
     * @throws NetworkException
     *         If sending the message failed or waiting for the response failed (timeout)
     */
    public void sendSync(final Request p_request, final int p_timeout, final boolean p_waitForResponses) throws NetworkException {
        // #if LOGGER == TRACE
        // LOGGER.trace("Sending request (sync): %s", p_request);
        // #endif /* LOGGER == TRACE */

        // #ifdef STATISTICS
        // SOP_SEND_SYNC.enter();
        // #endif /* STATISTICS */

        try {
            m_requestMap.put(p_request);
            sendMessage(p_request);
        } catch (final NetworkException e) {
            m_requestMap.remove(p_request.getRequestID());
            throw e;
        }

        // #if LOGGER == TRACE
        // LOGGER.trace("Waiting for response to request: %s", p_request);
        // #endif /* LOGGER == TRACE */

        int timeout = p_timeout != -1 ? p_timeout : m_timeOut;
        try {
            if (p_waitForResponses) {
                // #ifdef STATISTICS
                // SOP_WAIT_RESPONSE.enter();
                // #endif /* STATISTICS */

                p_request.waitForResponse(timeout);

                // #ifdef STATISTICS
                // SOP_WAIT_RESPONSE.leave();
                // #endif /* STATISTICS */

                // #ifdef STATISTICS
                // SOP_REQ_RESP_RTT.enter(p_request.getRoundTripTimeNs() / 1000);
                // #endif /* STATISTICS */

                // #ifdef STATISTICS
                // SOP_REQ_RESP_RTT.leave();
                // #endif /* STATISTICS */
            }
        } catch (final NetworkResponseDelayedException e) {
            // #ifdef STATISTICS
            // SOP_WAIT_RESPONSE.leave();
            // #endif /* STATISTICS */

            // #if LOGGER >= ERROR
            LOGGER.error("Sending sync, waiting for responses %s failed, timeout", p_request);
            // #endif /* LOGGER >= ERROR */

            m_requestMap.remove(p_request.getRequestID());

            throw e;
        } catch (final NetworkResponseCancelledException e) {
            // #ifdef STATISTICS
            // SOP_WAIT_RESPONSE.leave();
            // #endif /* STATISTICS */

            // #if LOGGER >= TRACE
            LOGGER.trace("Sending sync, waiting for responses %s failed, cancelled", p_request);
            // #endif /* LOGGER >= TRACE */

            throw e;
        }

        // #ifdef STATISTICS
        // SOP_SEND_SYNC.leave();
        // #endif /* STATISTICS */
    }

    /**
     * Cancel all pending requests waiting for a response. Also used on node failure, only
     *
     * @param p_nodeId
     *         Node id of the target node the requests
     *         are waiting for a response
     */
    public void cancelAllRequests(final short p_nodeId) {
        m_requestMap.removeAll(p_nodeId);
    }

}
