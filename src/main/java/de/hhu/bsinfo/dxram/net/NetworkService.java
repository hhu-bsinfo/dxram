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

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.engine.ComponentProvider;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;

/**
 * Service to access the backend network service for sending messages
 * to other participating nodes in the system.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
@Module.Attributes(supportsSuperpeer = false, supportsPeer = true)
public class NetworkService extends Service<ModuleConfig> {
    // component dependencies
    private NetworkComponent m_network;

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
     * @param p_type
     *         the message type
     * @param p_subtype
     *         the message subtype
     * @param p_receiver
     *         the receiver
     */
    public void registerReceiver(final byte p_type, final byte p_subtype, final MessageReceiver p_receiver) {
        m_network.register(p_type, p_subtype, p_receiver);
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
    public void unregisterReceiver(final byte p_type, final byte p_subtype, final MessageReceiver p_receiver) {
        m_network.unregister(p_type, p_subtype, p_receiver);
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
    public void sendSync(final Request p_request) throws NetworkException {
        m_network.sendSync(p_request);
    }

    @Override
    protected void resolveComponentDependencies(final ComponentProvider p_componentAccessor) {
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMConfig p_config) {
        return true;
    }

    @Override
    protected boolean shutdownService() {
        return true;
    }

}
