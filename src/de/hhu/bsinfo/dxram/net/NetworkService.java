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

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.AbstractRequest;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;

/**
 * Service to access the backend network service for sending messages
 * to other participating nodes in the system.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class NetworkService extends AbstractDXRAMService {

    // component dependencies
    private NetworkComponent m_network;

    /**
     * Constructor
     */
    public NetworkService() {
        super("net", true, true);
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
        m_network.registerMessageType(p_type, p_subtype, p_class);
    }

    /**
     * Registers a message receiver
     *
     * @param p_message
     *         the message
     * @param p_receiver
     *         the receiver
     */
    public void registerReceiver(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
        m_network.register(p_message, p_receiver);
    }

    /**
     * Unregisters a message receiver
     *
     * @param p_message
     *         the message
     * @param p_receiver
     *         the receiver
     */
    public void unregisterReceiver(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
        m_network.unregister(p_message, p_receiver);
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
        m_network.sendMessage(p_message);
    }

    /**
     * Send the Request and wait for fulfillment (wait for response).
     *
     * @param p_request
     *         The request to send.
     * @throws NetworkException
     *         If sending the message failed
     */
    public void sendSync(final AbstractRequest p_request) throws NetworkException {
        m_network.sendSync(p_request);
    }

    @Override
    protected boolean supportedBySuperpeer() {
        return true;
    }

    @Override
    protected boolean supportedByPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

}
