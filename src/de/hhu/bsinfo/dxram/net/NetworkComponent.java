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

package de.hhu.bsinfo.dxram.net;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.net.events.ConnectionLostEvent;
import de.hhu.bsinfo.dxram.net.events.ResponseDelayedEvent;
import de.hhu.bsinfo.dxram.net.messages.DefaultMessage;
import de.hhu.bsinfo.dxram.net.messages.NetworkMessages;
import de.hhu.bsinfo.net.MessageReceiver;
import de.hhu.bsinfo.net.NetworkDestinationUnreachableException;
import de.hhu.bsinfo.net.NetworkHandler;
import de.hhu.bsinfo.net.NetworkResponseDelayedException;
import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.AbstractRequest;
import de.hhu.bsinfo.net.core.ConnectionManagerListener;
import de.hhu.bsinfo.net.core.NetworkException;

/**
 * Access to the network interface to send messages or requests
 * to other nodes.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class NetworkComponent extends AbstractDXRAMComponent<NetworkComponentConfig> implements EventListener<NodeFailureEvent>, ConnectionManagerListener {
    // component dependencies
    private AbstractBootComponent m_boot;
    private EventComponent m_event;

    // Attributes
    private NetworkHandler m_networkHandler;

    /**
     * Constructor
     */
    public NetworkComponent() {
        super(DXRAMComponentOrder.Init.NETWORK, DXRAMComponentOrder.Shutdown.NETWORK, NetworkComponentConfig.class);
    }

    // --------------------------------------------------------------------------------------

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
        m_networkHandler.registerMessageType(p_type, p_subtype, p_class);
    }

    /**
     * Connect a node.
     *
     * @param p_nodeID
     *         Node to connect
     * @throws NetworkException
     *         If the destination is unreachable
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
     *         Message to send
     * @throws NetworkException
     *         If sending the message failed
     */
    public void sendMessage(final AbstractMessage p_message) throws NetworkException {
        // #if LOGGER == TRACE
        LOGGER.trace("Sending message %s", p_message);
        // #endif /* LOGGER == TRACE */

        try {
            m_networkHandler.sendMessage(p_message);
        } catch (final NetworkDestinationUnreachableException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending message %s failed: %s", p_message.getClass().getSimpleName(), e);
            // #endif /* LOGGER >= ERROR */

            // Connection creation failed -> trigger failure handling
            m_event.fireEvent(new ConnectionLostEvent(getClass().getSimpleName(), p_message.getDestination()));

            throw e;
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Sending message %s failed: %s", p_message, e);
            // #endif /* LOGGER >= ERROR */

            throw e;
        }
    }

    /**
     * Send the Request and wait for fulfillment (wait for response, default timeout).
     *
     * @param p_request
     *         The request to send.
     * @throws NetworkException
     *         If sending the message failed
     */
    public void sendSync(final AbstractRequest p_request) throws NetworkException {
        sendSync(p_request, true);
    }

    /**
     * Send the Request and wait for fulfillment (wait for response, specific timeout).
     *
     * @param p_request
     *         The request to send.
     * @param p_timeout
     *         The amount of time to wait for a response
     * @throws NetworkException
     *         If sending the message failed
     */
    public void sendSync(final AbstractRequest p_request, final int p_timeout) throws NetworkException {
        try {
            m_networkHandler.sendSync(p_request, p_timeout, true);
        } catch (final NetworkDestinationUnreachableException e) {
            m_event.fireEvent(new ConnectionLostEvent(getClass().getSimpleName(), p_request.getDestination()));

            throw e;
        } catch (final NetworkResponseDelayedException e) {
            m_event.fireEvent(new ResponseDelayedEvent(getClass().getSimpleName(), e.getDesinationNodeId()));

            throw e;
        }
    }

    /**
     * Send the Request and wait for fulfillment (wait for response if corresponding parameter is true).
     *
     * @param p_request
     *         The request to send.
     * @param p_waitForResponses
     *         Set to false to not wait/block until the response arrived
     * @throws NetworkException
     *         If sending the message failed
     */
    public void sendSync(final AbstractRequest p_request, final boolean p_waitForResponses) throws NetworkException {

        try {
            m_networkHandler.sendSync(p_request, -1, p_waitForResponses);
        } catch (final NetworkDestinationUnreachableException e) {
            m_event.fireEvent(new ConnectionLostEvent(getClass().getSimpleName(), p_request.getDestination()));

            throw e;
        } catch (final NetworkResponseDelayedException e) {
            m_event.fireEvent(new ResponseDelayedEvent(getClass().getSimpleName(), e.getDesinationNodeId()));

            throw e;
        }
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
        m_networkHandler.register(p_type, p_subtype, p_receiver);
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
        m_networkHandler.unregister(p_type, p_subtype, p_receiver);
    }

    // --------------------------------------------------------------------------------------

    @Override
    public void eventTriggered(final NodeFailureEvent p_event) {
        // #if LOGGER >= DEBUG
        LOGGER.debug("Connection to peer 0x%X lost, aborting and removing all pending requests", p_event.getNodeID());
        // #endif /* LOGGER >= DEBUG */

        m_networkHandler.cancelAllRequests(p_event.getNodeID());
    }

    @Override
    public void connectionCreated(final short p_destination) {
        // #if LOGGER >= DEBUG
        LOGGER.debug("Connection to node 0x%X created", p_destination);
        // #endif /* LOGGER >= DEBUG */
    }

    @Override
    public void connectionLost(final short p_destination) {
        // #if LOGGER >= DEBUG
        LOGGER.debug("Connection to node 0x%X lost", p_destination);
        // #endif /* LOGGER >= DEBUG */

        m_event.fireEvent(new ConnectionLostEvent(getClass().getSimpleName(), p_destination));
    }

    @Override
    protected boolean supportsSuperpeer() {
        return true;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_event = p_componentAccessor.getComponent(EventComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMContext.Config p_config) {
        if (!getConfig().isInfiniband()) {
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
        }

        m_networkHandler = new NetworkHandler(m_boot.getNodeID(), getConfig().getMaxConnections(), (int) getConfig().getBufferSize().getBytes(),
                (int) getConfig().getFlowControlWindowSize().getBytes(), getConfig().getNumMessageHandlerThreads(), getConfig().getRequestMapEntryCount(),
                (int) getConfig().getRequestTimeout().getMs(), (int) getConfig().getConnectionTimeout().getMs(), new NodeMappings(m_boot),
                getConfig().isInfiniband());

        m_networkHandler.setConnectionManagerListener(this);
        m_networkHandler.registerMessageType(DXRAMMessageTypes.NETWORK_MESSAGES_TYPE, NetworkMessages.SUBTYPE_DEFAULT_MESSAGE, DefaultMessage.class);

        m_event.registerListener(this, NodeFailureEvent.class);

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_networkHandler.close();

        m_networkHandler = null;

        return true;
    }
}
