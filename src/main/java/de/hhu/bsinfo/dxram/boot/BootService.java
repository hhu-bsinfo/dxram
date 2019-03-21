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

package de.hhu.bsinfo.dxram.boot;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.util.List;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.messages.BootMessages;
import de.hhu.bsinfo.dxram.boot.messages.ShutdownMessage;
import de.hhu.bsinfo.dxutils.dependency.Dependency;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.engine.ComponentProvider;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Service providing information about the bootstrapping process like
 * node ids, node roles, addresses etc.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 * @author Filip Krakowski, Filip.Krakowski@hhu.de, 18.05.2018
 */
@Module.Attributes(supportsSuperpeer = true, supportsPeer = true)
public class BootService extends Service<ModuleConfig> implements MessageReceiver {

    @Dependency
    private BootComponent m_boot;

    @Dependency
    private NetworkComponent m_network;

    /**
     * Handler an incoming ShutdownMessage.
     *
     * @param p_message
     *         Message to handle.
     */
    private static void incomingShutdownMessage(final ShutdownMessage p_message) {
        shutdown(p_message.isHardShutdown());
    }

    /**
     * Shutdown the current node.
     *
     * @param p_hardShutdown
     *         True to kill the node without shutting down DXRAM, false for proper DXRAM shutdown.
     */
    private static void shutdown(final boolean p_hardShutdown) {
        if (p_hardShutdown) {
            // suicide
            // note: this might not work correctly on every jvm implementation
            String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
            try {
                Runtime.getRuntime().exec("kill -9 " + pid);
            } catch (final IOException ignored) {

            }

        } else {
            // triggers the registered cleanup handler
            System.exit(0);
        }
    }

    /**
     * Get the ID of the node, you are currently running on.
     *
     * @return NodeID.
     */
    public short getNodeID() {
        return m_boot.getNodeId();
    }

    /**
     * Get IDs of all available (online) nodes including own.
     *
     * @return List of IDs of nodes available.
     */
    public List<Short> getOnlineNodeIDs() {
        return m_boot.getOnlineNodeIds();
    }

    /**
     * Get the node role of the current node.
     *
     * @return Node role of current node.
     */
    public NodeRole getNodeRole() {
        return m_boot.getNodeRole();
    }

    /**
     * Get IDs of all available (online) peer nodes exception our own.
     *
     * @return List of IDs of nodes available.
     */
    public List<Short> getOnlineSuperpeerNodeIDs() {
        return m_boot.getOnlineSuperpeerIds();
    }

    /**
     * Get IDs of all available (online) peer nodes exception our own.
     *
     * @return List of IDs of nodes available.
     */
    public List<Short> getOnlinePeerNodeIDs() {
        return m_boot.getOnlinePeerIds();
    }

    /**
     * Collects all node ids supporting the specified capabilities.
     *
     * @param p_capabilities
     *         The requested capabilities.
     * @return A list containing all matching node ids.
     */
    public List<Short> getSupportingNodes(int p_capabilities) {
        return m_boot.getSupportingNodes(p_capabilities);
    }

    /**
     * Check if a specific node is online.
     *
     * @param p_nodeID
     *         Node to check.
     * @return True if online, false offline.
     */
    public boolean isNodeOnline(final short p_nodeID) {
        return m_boot.isNodeOnline(p_nodeID);
    }

    /**
     * Get the role of another nodeID.
     *
     * @param p_nodeID
     *         Node id to get the role of.
     * @return Role of other nodeID or null if node does not exist.
     */
    public NodeRole getNodeRole(final short p_nodeID) {
        return m_boot.getNodeRole(p_nodeID);
    }

    /**
     * Returns the specified node's capabilities.
     *
     * @param p_nodeId
     *         The node's id.
     * @return The specified node's capabilities.
     */
    public int getNodeCapabilities(final short p_nodeId) {
        return m_boot.getNodeCapabilities(p_nodeId);
    }

    /**
     * Get the IP and port of another node.
     *
     * @param p_nodeID
     *         Node ID of the node.
     * @return IP and port of the specified node or an invalid address if not available.
     */
    public InetSocketAddress getNodeAddress(final short p_nodeID) {
        return m_boot.getNodeAddress(p_nodeID);
    }

    /**
     * Shutdown a single node or all nodes.
     *
     * @param p_nodeID
     *         Node id to shut down or -1/0xFFFF to shut down all nodes.
     * @param p_hardShutdown
     *         If true this will kill the process instead of shutting down DXRAM properly.
     * @return True if successful, false on failure.
     */
    public boolean shutdownNode(final short p_nodeID, final boolean p_hardShutdown) {
        if (p_nodeID == NodeID.INVALID_ID) {
            List<Short> nodeIds = m_boot.getOnlineNodeIds();

            // shutdown peers first
            for (Short nodeId : nodeIds) {
                if (nodeId != m_boot.getNodeId() && m_boot.getNodeRole(nodeId) == NodeRole.PEER) {
                    ShutdownMessage message = new ShutdownMessage(nodeId, p_hardShutdown);
                    try {
                        m_network.sendMessage(message);
                    } catch (final NetworkException e) {

                        LOGGER.error("Shutting down node %s failed: %s", NodeID.toHexString(nodeId), e);

                        return false;
                    }
                }
            }

            // some delay so peers still have their superpeers when shutting down
            try {
                Thread.sleep(5000);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }

            // then superpeers
            for (Short nodeId : nodeIds) {
                if (nodeId != m_boot.getNodeId() && m_boot.getNodeRole(nodeId) == NodeRole.SUPERPEER) {
                    ShutdownMessage message = new ShutdownMessage(nodeId, p_hardShutdown);

                    try {
                        m_network.sendMessage(message);
                    } catch (final NetworkException e) {

                        LOGGER.error("Shutting down node %s failed: %s", NodeID.toHexString(nodeId), e);

                        return false;
                    }
                }
            }

            // and ourselves
            shutdown(p_hardShutdown);

        } else {
            if (p_nodeID == m_boot.getNodeId()) {
                shutdown(p_hardShutdown);
            } else {
                ShutdownMessage message = new ShutdownMessage(p_nodeID, p_hardShutdown);

                try {
                    m_network.sendMessage(message);
                } catch (final NetworkException e) {

                    LOGGER.error("Shutting down node %s failed: %s", NodeID.toHexString(p_nodeID), e);

                    return false;
                }

                LOGGER.info("Sent remote shutdown to node %s", NodeID.toHexString(p_nodeID));

            }
        }

        return true;
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message != null) {
            if (p_message.getType() == DXRAMMessageTypes.BOOT_MESSAGES_TYPE) {
                switch (p_message.getSubtype()) {
                    case BootMessages.SUBTYPE_SHUTDOWN_MESSAGE:
                        incomingShutdownMessage((ShutdownMessage) p_message);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    @Override
    protected boolean startService(final DXRAMConfig p_config) {
        m_network.registerMessageType(DXRAMMessageTypes.BOOT_MESSAGES_TYPE, BootMessages.SUBTYPE_SHUTDOWN_MESSAGE,
                ShutdownMessage.class);

        m_network.register(DXRAMMessageTypes.BOOT_MESSAGES_TYPE, BootMessages.SUBTYPE_SHUTDOWN_MESSAGE, this);

        return true;
    }

    @Override
    protected boolean shutdownService() {
        m_network.unregister(DXRAMMessageTypes.BOOT_MESSAGES_TYPE, BootMessages.SUBTYPE_SHUTDOWN_MESSAGE, this);

        return true;
    }
}
