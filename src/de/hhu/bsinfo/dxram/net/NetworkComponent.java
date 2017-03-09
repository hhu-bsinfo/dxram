/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.net;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import com.google.gson.annotations.Expose;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.net.events.ConnectionLostEvent;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.net.messages.DefaultMessage;
import de.hhu.bsinfo.dxram.net.messages.DefaultMessages;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.AbstractRequest;
import de.hhu.bsinfo.ethnet.NetworkDestinationUnreachableException;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.ethnet.NetworkResponseTimeoutException;
import de.hhu.bsinfo.ethnet.RequestMap;
import de.hhu.bsinfo.utils.unit.StorageUnit;
import de.hhu.bsinfo.utils.unit.TimeUnit;

/**
 * Access to the network interface to send messages or requests
 * to other nodes.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class NetworkComponent extends AbstractDXRAMComponent {

    private static final Logger LOGGER = LogManager.getFormatterLogger(NetworkComponent.class.getSimpleName());

    // configuration values
    @Expose
    private int m_threadCountMsgHandler = 1;
    @Expose
    private int m_requestMapEntryCount = (int) Math.pow(2, 20);
    @Expose
    private StorageUnit m_incomingBufferSize = new StorageUnit(1, StorageUnit.MB);
    @Expose
    private StorageUnit m_outgoingBufferSize = new StorageUnit(1, StorageUnit.MB);
    @Expose
    private int m_numberOfPendingBuffersPerConnection = 100;
    @Expose
    private StorageUnit m_flowControlWindowSize = new StorageUnit(1, StorageUnit.MB);
    @Expose
    private TimeUnit m_requestTimeout = new TimeUnit(333, TimeUnit.MS);

    // dependent components
    private AbstractBootComponent m_boot;
    private EventComponent m_event;

    // Attributes
    private NetworkHandler m_networkHandler;

    /**
     * Constructor
     */
    public NetworkComponent() {
        super(DXRAMComponentOrder.Init.NETWORK, DXRAMComponentOrder.Shutdown.NETWORK);
    }

    // --------------------------------------------------------------------------------------

    /**
     * Activates the connection manager
     */
    public void activateConnectionManager() {
        m_networkHandler.activateConnectionManager();
    }

    /**
     * Deactivates the connection manager
     */
    public void deactivateConnectionManager() {
        m_networkHandler.deactivateConnectionManager();
    }

    /**
     * Registers a message type
     *
     * @param p_type
     *     the unique type
     * @param p_subtype
     *     the unique subtype
     * @param p_class
     *     the calling class
     */
    public void registerMessageType(final byte p_type, final byte p_subtype, final Class<?> p_class) {
        m_networkHandler.registerMessageType(p_type, p_subtype, p_class);
    }

    /**
     * Connect a node.
     *
     * @param p_nodeID
     *     Node to connect
     * @throws NetworkException
     *     If the destination is unreachable
     */
    public void connectNode(final short p_nodeID) throws NetworkException {
        // #if LOGGER == TRACE
        LOGGER.trace("Connecting node 0x%X", p_nodeID);
        // #endif /* LOGGER == TRACE */

        try {
            m_networkHandler.connectNode(p_nodeID);
        } catch (final NetworkDestinationUnreachableException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Connecting node 0x%X failed: %s", p_nodeID, e);
            // #endif /* LOGGER >= ERROR */
            throw e;
        }
    }

    /**
     * Send a message.
     *
     * @param p_message
     *     Message to send
     * @throws NetworkException
     *     If sending the message failed
     */
    public void sendMessage(final AbstractMessage p_message) throws NetworkException {
        // #if LOGGER == TRACE
        LOGGER.trace("Sending message %s", p_message);
        // #endif /* LOGGER == TRACE */

        try {
            m_networkHandler.sendMessage(p_message);
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending message %s failed: %s", p_message, e);
            // #endif /* LOGGER >= ERROR */

            if (e instanceof NetworkDestinationUnreachableException) {
                // Connection creation failed -> trigger failure handling
                m_event.fireEvent(new ConnectionLostEvent(getClass().getSimpleName(), p_message.getDestination()));
            }

            throw e;
        }
    }

    /**
     * Send the Request and wait for fulfillment (wait for response).
     *
     * @param p_request
     *     The request to send.
     * @throws NetworkException
     *     If sending the message failed
     */
    public void sendSync(final AbstractRequest p_request) throws NetworkException {
        sendSync(p_request, true);
    }

    /**
     * Send the Request and wait for fulfillment (wait for response).
     *
     * @param p_request
     *     The request to send.
     * @param p_waitForResponses
     *     Set to false to not wait/block until the response arrived
     * @throws NetworkException
     *     If sending the message failed
     */
    public void sendSync(final AbstractRequest p_request, final boolean p_waitForResponses) throws NetworkException {
        // #if LOGGER == TRACE
        LOGGER.trace("Sending request (sync): %s", p_request);
        // #endif /* LOGGER == TRACE */

        try {
            sendMessage(p_request);
        } catch (final NetworkException e) {
            RequestMap.remove(p_request.getRequestID());
            throw e;
        }

        // #if LOGGER == TRACE
        LOGGER.trace("Waiting for response to request: %s", p_request);
        // #endif /* LOGGER == TRACE */

        if (p_waitForResponses && !p_request.waitForResponses((int) m_requestTimeout.getMs())) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending sync, waiting for responses %s failed, timeout", p_request);
            // #endif /* LOGGER >= ERROR */

            // #if LOGGER >= DEBUG
            LOGGER.debug(m_networkHandler.getStatus());
            // #endif /* LOGGER >= DEBUG */

            RequestMap.remove(p_request.getRequestID());

            throw new NetworkResponseTimeoutException(p_request.getDestination());
        }
    }

    /**
     * Registers a message receiver
     *
     * @param p_message
     *     the message
     * @param p_receiver
     *     the receiver
     */
    public void register(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
        m_networkHandler.register(p_message, p_receiver);
    }

    /**
     * Unregisters a message receiver
     *
     * @param p_message
     *     the message
     * @param p_receiver
     *     the receiver
     */
    public void unregister(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
        m_networkHandler.unregister(p_message, p_receiver);
    }

    // --------------------------------------------------------------------------------------

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_event = p_componentAccessor.getComponent(EventComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        m_networkHandler = new NetworkHandler(m_threadCountMsgHandler, m_requestMapEntryCount);
        NetworkHandler.setEventHandler(m_event);

        // Check if given ip address is bound to one of this node's network interfaces
        boolean found = false;
        InetAddress myAddress = m_boot.getNodeAddress(m_boot.getNodeID()).getAddress();
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            outerloop:
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface currentNetworkInterface = networkInterfaces.nextElement();
                Enumeration<InetAddress> addresses = currentNetworkInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress currentAddress = addresses.nextElement();
                    if (myAddress.equals(currentAddress)) {
                        // #if LOGGER >= INFO
                        LOGGER.info("%s is bound to %s", myAddress.getHostAddress(), currentNetworkInterface.getDisplayName());
                        // #endif /* LOGGER >= INFO */
                        found = true;
                        break outerloop;
                    }
                }
            }
        } catch (final SocketException e1) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not get network interfaces for ip confirmation");
            // #endif /* LOGGER >= ERROR */
        } finally {
            if (!found) {
                // #if LOGGER >= ERROR
                LOGGER.error("Could not find network interface with address %s", myAddress.getHostAddress());
                // #endif /* LOGGER >= ERROR */
                return false;
            }
        }

        m_networkHandler.initialize(m_boot.getNodeID(), new NodeMappings(m_boot), (int) m_incomingBufferSize.getBytes(), (int) m_outgoingBufferSize.getBytes(),
            m_numberOfPendingBuffersPerConnection, (int) m_flowControlWindowSize.getBytes(), (int) m_requestTimeout.getMs());

        m_networkHandler.registerMessageType(DXRAMMessageTypes.DEFAULT_MESSAGES_TYPE, DefaultMessages.SUBTYPE_DEFAULT_MESSAGE, DefaultMessage.class);

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_networkHandler.close();

        m_networkHandler = null;

        return true;
    }
}
