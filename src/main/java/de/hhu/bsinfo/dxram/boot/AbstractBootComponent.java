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

import java.net.InetSocketAddress;
import java.util.List;

import de.hhu.bsinfo.dxram.backup.BackupPeer;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * Component executing the bootstrapping of a node in DXRAM.
 * It takes care of assigning the node ID to this node, its role and
 * managing everything related to the basic node status (available,
 * failure report...)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 * @author Filip Krakowski, Filip.Krakowski@hhu.de, 18.05.2018
 */
public abstract class AbstractBootComponent<T> extends AbstractDXRAMComponent<T> {
    /**
     * Register a node registry listener
     *
     * @param p_listener
     *         Listener to register
     */
    public abstract void registerRegistryListener(final NodeRegistry.Listener p_listener);

    /**
     * Returns this node's details.
     *
     * @return This node's details.
     */
    public abstract NodeRegistry.NodeDetails getDetails();

    /**
     * Returns the specified node's details.
     *
     * @return This specified node's details.
     */
    public abstract NodeRegistry.NodeDetails getDetails(final short p_nodeId);

    /**
     * Get node entries of all available (online) nodes including the own.
     *
     * @return List of IDs of nodes available.
     */
    public abstract List<NodeRegistry.NodeDetails> getOnlineNodes();

    /**
     * Get IDs of all available (online) nodes including the own.
     *
     * @return List of IDs of nodes available.
     */
    public abstract List<Short> getOnlineNodeIds();

    /**
     * Get IDs of all available (online) peer nodes except the own.
     *
     * @return List of IDs of nodes available without own ID.
     */
    public abstract List<Short> getOnlinePeerIds();

    /**
     * Get IDs of all available (online) superpeer nodes except the own.
     *
     * @return List of IDs of nodes available without own ID.
     */
    public abstract List<Short> getOnlineSuperpeerIds();

    /**
     * Get IDs of all available (online) backup peers.
     *
     * @return List of IDs of peers available for backup without own ID.
     */
    public abstract List<BackupPeer> getAvailableBackupPeers();

    /**
     * Collects all node ids supporting the specified capabilities.
     *
     * @param p_capabilities
     *         The requested capabilities.
     * @return A list containing all matching node ids.
     */
    public abstract List<Short> getSupportingNodes(int p_capabilities);

    /**
     * Updates this node's capabilities.
     *
     * @param p_capibilities
     *         The updated capabilities.
     */
    public abstract void updateNodeCapabilities(int p_capibilities);

    /**
     * Get the node ID of the currently set bootstrap node.
     *
     * @return Node ID assigned for bootstrapping or -1 if no bootstrap assigned/available.
     */
    public abstract short getBootstrapId();

    public abstract short getNodeId();

    public abstract NodeRole getNodeRole();

    public abstract short getRack();

    public abstract short getSwitch();

    public abstract int getNumberOfAvailableSuperpeers();

    public abstract InetSocketAddress getNodeAddress(final short p_nodeId);

    public abstract NodeRole getNodeRole(final short p_nodeId);

    public abstract boolean isNodeOnline(final short p_nodeId);

    public abstract int getNodeCapabilities(final short p_nodeId);
}
