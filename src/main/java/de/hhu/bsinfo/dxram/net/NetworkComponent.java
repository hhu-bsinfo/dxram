/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.net;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import de.hhu.bsinfo.dxnet.ConnectionManagerListener;
import de.hhu.bsinfo.dxnet.DXNet;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.NetworkDestinationUnreachableException;
import de.hhu.bsinfo.dxnet.NetworkResponseDelayedException;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxnet.core.messages.Messages;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMModule;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.net.events.ConnectionLostEvent;
import de.hhu.bsinfo.dxram.net.events.ResponseDelayedEvent;

/**
 * Access to the network interface to send messages or requests
 * to other nodes.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
@AbstractDXRAMModule.Attributes(supportsSuperpeer = true, supportsPeer = true)
@AbstractDXRAMComponent.Attributes(priorityInit = DXRAMComponentOrder.Init.NETWORK,
        priorityShutdown = DXRAMComponentOrder.Shutdown.NETWORK)
public class NetworkComponent extends AbstractDXRAMComponent<NetworkComponentConfig>
        implements EventListener<NodeFailureEvent>, ConnectionManagerListener {
    // component dependencies
    private AbstractBootComponent m_boot;
    private EventComponent m_event;

    // Attributes
    private DXNet m_dxnet;

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
        if (p_type == Messages.DEFAULT_MESSAGES_TYPE) {
            LOGGER.error("Registering network message %s for type %s and subtype %s failed, type 0 is used for " +
                    "internal messages and not allowed", p_class.getSimpleName(), p_type, p_subtype);
            return;
        }

        m_dxnet.registerMessageType(p_type, p_subtype, p_class);
    }

    /**
     * Registers a special receive message type
     *
     * @param p_type
     *         the unique type
     * @param p_subtype
     *         the unique subtype
     */
    public void registerSpecialReceiveMessageType(final byte p_type, final byte p_subtype) {
        m_dxnet.registerSpecialReceiveMessageType(p_type, p_subtype);
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
        LOGGER.trace("Connecting node 0x%X", p_nodeID);

        try {
            m_dxnet.connectNode(p_nodeID);
        } catch (final NetworkDestinationUnreachableException e) {
            LOGGER.error("Connecting node 0x%X failed: %s", p_nodeID, e);
            throw e;
        }
    }

    /**
     * The configured timeout in ms for requests
     *
     * @return Timeout in ms
     */
    public int getRequestTimeoutMs() {
        return m_dxnet.getRequestTimeoutMs();
    }

    /**
     * Send a message.
     *
     * @param p_message
     *         Message to send
     * @throws NetworkException
     *         If sending the message failed
     */
    public void sendMessage(final Message p_message) throws NetworkException {
        LOGGER.trace("Sending message %s", p_message);

        try {
            m_dxnet.sendMessage(p_message);
        } catch (final NetworkDestinationUnreachableException e) {
            LOGGER.error("Sending message %s failed: %s", p_message.getClass().getSimpleName(), e);

            // Connection creation failed -> trigger failure handling
            m_event.fireEvent(new ConnectionLostEvent(getClass().getSimpleName(), p_message.getDestination()));

            throw e;
        } catch (final NetworkException e) {
            LOGGER.error("Sending message %s failed: %s", p_message, e);
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
    public void sendSync(final Request p_request) throws NetworkException {
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
    public void sendSync(final Request p_request, final int p_timeout) throws NetworkException {
        try {
            m_dxnet.sendSync(p_request, p_timeout, true);
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
    public void sendSync(final Request p_request, final boolean p_waitForResponses) throws NetworkException {

        try {
            m_dxnet.sendSync(p_request, -1, p_waitForResponses);
        } catch (final NetworkDestinationUnreachableException e) {
            m_event.fireEvent(new ConnectionLostEvent(getClass().getSimpleName(), p_request.getDestination()));

            throw e;
        } catch (final NetworkResponseDelayedException e) {
            m_event.fireEvent(new ResponseDelayedEvent(getClass().getSimpleName(), e.getDesinationNodeId()));

            throw e;
        }
    }

    /**
     * Cancel a pending request. This deletes the request from the request map to ensure
     * that any delayed incoming responses are automatically dropped
     *
     * @param p_request Request to cancel
     */
    public void cancelRequest(final Request p_request) {
        m_dxnet.cancelRequest(p_request);
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
        m_dxnet.register(p_type, p_subtype, p_receiver);
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
        m_dxnet.unregister(p_type, p_subtype, p_receiver);
    }

    // --------------------------------------------------------------------------------------

    @Override
    public void eventTriggered(final NodeFailureEvent p_event) {
        LOGGER.debug("Connection to peer 0x%X lost, aborting and removing all pending requests", p_event.getNodeID());
        m_dxnet.cancelAllRequests(p_event.getNodeID());
    }

    @Override
    public void connectionCreated(final short p_destination) {
        LOGGER.debug("Connection to node 0x%X created", p_destination);
    }

    @Override
    public void connectionLost(final short p_destination) {
        LOGGER.debug("Connection to node 0x%X lost", p_destination);
        m_event.fireEvent(new ConnectionLostEvent(getClass().getSimpleName(), p_destination));
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_event = p_componentAccessor.getComponent(EventComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMConfig p_config, final DXRAMJNIManager p_jniManager) {
        // node id is not loaded from config
        getConfig().getCoreConfig().setOwnNodeId(m_boot.getNodeId());

        switch (getConfig().getCoreConfig().getDevice()) {
            case ETHERNET: {
                // Check if given ip address is bound to one of this node's network interfaces
                boolean found = false;
                InetAddress myAddress = m_boot.getNodeAddress(m_boot.getNodeId()).getAddress();
                try {
                    Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
                    outerloop:
                    while (networkInterfaces.hasMoreElements()) {
                        NetworkInterface currentNetworkInterface = networkInterfaces.nextElement();
                        Enumeration<InetAddress> addresses = currentNetworkInterface.getInetAddresses();
                        while (addresses.hasMoreElements()) {
                            InetAddress currentAddress = addresses.nextElement();
                            if (myAddress.equals(currentAddress)) {

                                LOGGER.debug("%s is bound to %s", myAddress.getHostAddress(),
                                        currentNetworkInterface.getDisplayName());

                                found = true;
                                break outerloop;
                            }
                        }
                    }
                } catch (final SocketException ignored) {
                    LOGGER.error("Could not get network interfaces for ip confirmation");
                } finally {
                    if (!found) {
                        LOGGER.error("Could not find network interface with address %s", myAddress.getHostAddress());
                        return false;
                    }
                }

                break;
            }

            case INFINIBAND: {
                if (!p_jniManager.loadJNIModule("MsgrcJNIBinding")) {
                    return false;
                }

                break;
            }
        }

        m_dxnet = new DXNet(getConfig().getCoreConfig(), getConfig().getNioConfig(), getConfig().getIbConfig(), null,
                new NodeMappings(m_boot));

        m_dxnet.setConnectionManagerListener(this);

        m_event.registerListener(this, NodeFailureEvent.class);

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_dxnet.close();

        m_dxnet = null;

        return true;
    }
}
