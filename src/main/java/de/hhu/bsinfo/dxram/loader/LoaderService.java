/*
 * Copyright (C) 2019 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
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

package de.hhu.bsinfo.dxram.loader;

import java.nio.file.Path;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.loader.messages.FlushTableMessage;
import de.hhu.bsinfo.dxram.loader.messages.LoaderMessages;
import de.hhu.bsinfo.dxram.loader.messages.TableCountRequest;
import de.hhu.bsinfo.dxram.loader.messages.TableCountResponse;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.dependency.Dependency;

/**
 * @author Julien Bernhart, julien.bernhart@hhu.de, 2019-04-17
 */
@Module.Attributes(supportsSuperpeer = true, supportsPeer = true)
public class LoaderService extends Service<ModuleConfig> implements MessageReceiver {
    @Dependency
    private LoaderComponent m_loader;

    @Dependency
    private NetworkComponent m_net;

    @Dependency
    private BootComponent m_boot;

    private NodeRole m_role;

    /**
     * Get the DistributedLoader
     *
     * @return DistributedLoader instance
     */
    public DistributedLoader getClassLoader() {
        return m_loader.getM_loader();
    }

    /**
     * Add jar from specific path to the DistributedLoader
     *
     * @param p_jarPath path of the file to be added
     */
    public void addJar(Path p_jarPath) {
        m_loader.addJarToLoader(p_jarPath);
    }

    /**
     * Find specific class in cluster
     *
     * @param p_name name of the class
     * @return the class
     * @throws ClassNotFoundException
     */
    public Class<?> findClass(String p_name) throws ClassNotFoundException{
        return m_loader.getM_loader().findClass(p_name);
    }

    /**
     * Get number of loaded entries, if the node is a superpeer
     *
     * @return number of loaded entries in LoaderTable
     */
    public int getLoadedCount() {
        return m_loader.getLocalLoadedCount();
    }

    /**
     * Get number of loaded entries of specific superpeer
     *
     * @param p_nid nid of superpeer
     * @return number of loaded entries in LoaderTable
     */
    public int getLoadedCount(short p_nid) {
        TableCountRequest tableCountRequest = new TableCountRequest(p_nid);

        try {
            m_net.sendSync(tableCountRequest, true);
            TableCountResponse response = (TableCountResponse) tableCountRequest.getResponse();

            return response.getM_tableCount();
        } catch (NetworkException e) {
            LOGGER.error(e);
        }

        return -1;
    }

    /**
     * Flush LoaderTable of specific superpeer
     *
     * @param p_nid superpeer to flush
     */
    public void flushSuperpeerTable(short p_nid) {
        FlushTableMessage flushTableMessage = new FlushTableMessage(p_nid);
        try {
            m_net.sendMessage(flushTableMessage);
        } catch (NetworkException e) {
            LOGGER.error(e);
        }
    }

    /**
     * Flush local LoaderTable, if the node is a superpeer.
     */
    public void flushSuperpeerTable() {
        m_loader.flushTable();
    }

    public void sync() {
        m_loader.sync();
    }

    @Override
    protected boolean startService(DXRAMConfig p_config) {
        m_role = p_config.getEngineConfig().getRole();

        m_net.registerMessageType(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_FLUSH_TABLE,
                FlushTableMessage.class);
        m_net.registerMessageType(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_TABLE_COUNT_RESPONSE,
                TableCountResponse.class);
        m_net.registerMessageType(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_TABLE_COUNT_REQUEST,
                TableCountRequest.class);

        if (m_role == NodeRole.SUPERPEER) {
            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_FLUSH_TABLE, this);
            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_TABLE_COUNT_RESPONSE, this);
            m_net.register(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_TABLE_COUNT_REQUEST, this);
        }

        return true;
    }

    @Override
    protected boolean shutdownService() {
        if (m_role == NodeRole.SUPERPEER) {
            m_net.unregister(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_FLUSH_TABLE, this);
            m_net.unregister(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_TABLE_COUNT_RESPONSE, this);
            m_net.unregister(DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_TABLE_COUNT_REQUEST, this);
        }

        return true;
    }

    /**
     * Flush LoaderTable
     *
     * @param p_message message with request
     */
    private void onIncomingFlushMessage(Message p_message) {
        m_loader.flushTable();
        LOGGER.info(String.format("Flushed LoaderTable on superpeer %s", NodeID.toHexString(m_boot.getNodeId())));
    }

    private void onIncomingTableCountRequest(Message p_message) {
        TableCountResponse tableCountResponse = new TableCountResponse((TableCountRequest) p_message,
                m_loader.getLocalLoadedCount());

        try {
            m_net.sendMessage(tableCountResponse);
        } catch (NetworkException e) {
            LOGGER.error(e);
        }
    }

    @Override
    public void onIncomingMessage(Message p_message) {
        switch (p_message.getSubtype()) {
            case LoaderMessages.SUBTYPE_FLUSH_TABLE:
                onIncomingFlushMessage(p_message);
                break;
            case LoaderMessages.SUBTYPE_TABLE_COUNT_REQUEST:
                onIncomingTableCountRequest(p_message);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + p_message.getSubtype());
        }
    }
}
