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

package de.hhu.bsinfo.dxram.boot;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.NodeID;
import de.hhu.bsinfo.utils.unit.IPV4Unit;

/**
 * Represents a nodes configuration for DXRAM. This also holds any information
 * about the current node as well as any remote nodes available in the system.
 *
 * @author Florian Klein, florian.klein@hhu.de, 03.09.2013
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 9.12.2015
 */
final class NodesConfiguration {

    private NodeEntry[] m_nodes = new NodeEntry[NodeID.MAX_ID + 1];
    private short m_ownID = NodeID.INVALID_ID;

    /**
     * Creates an instance of NodesConfiguration
     */
    NodesConfiguration() {

    }

    /**
     * Gets the configured node
     *
     * @return the configured nodes
     */
    NodeEntry[] getNodes() {
        return m_nodes;
    }

    /**
     * Get the node ID which is set for this node.
     *
     * @return Own node ID (or -1 if invalid).
     */
    short getOwnNodeID() {
        return m_ownID;
    }

    /**
     * Set the node ID for the current/own node.
     *
     * @param p_nodeID
     *     Node id to set.
     */
    void setOwnNodeID(final short p_nodeID) {
        m_ownID = p_nodeID;
    }

    /**
     * Get the NodeEntry corresponding to our node ID.
     *
     * @return NodeEntry or null if invalid.
     */
    NodeEntry getOwnNodeEntry() {
        return m_nodes[m_ownID & 0xFFFF];
    }

    // ---------------------------------------------------------------------------

    @Override
    public String toString() {
        String str = "";

        str += "NodesConfiguration[ownID: " + m_ownID + "]:";
        for (int i = 0; i < m_nodes.length; i++) {
            if (m_nodes[i] != null) {
                str += '\n' + NodeID.toHexString((short) i) + ": " + m_nodes[i];
            }
        }

        return str;
    }

    /**
     * Get the NodeEntry of the specified node ID.
     *
     * @param p_nodeID
     *     Node ID to get the entry of.
     * @return NodeEntry containing information about the node or null if it does not exist.
     */
    NodeEntry getNode(final short p_nodeID) {
        return m_nodes[p_nodeID & 0xFFFF];
    }

    /**
     * Adds a node
     *
     * @param p_nodeID
     *     Id of the node.
     * @param p_entry
     *     the configured node
     * @return whether this is a new entry or not
     */
    synchronized boolean addNode(final short p_nodeID, final NodeEntry p_entry) {
        NodeEntry prev = m_nodes[p_nodeID & 0xFFFF];
        m_nodes[p_nodeID & 0xFFFF] = p_entry;

        return prev == null || !prev.getAddress().equals(p_entry.getAddress());
    }

    /**
     * Remove a node from the mappings list.
     *
     * @param p_nodeID
     *     Node ID of the entry to remove.
     */
    synchronized void removeNode(final short p_nodeID) {
        m_nodes[p_nodeID & 0xFFFF] = null;
    }

    /**
     * Describes a nodes configuration entry
     *
     * @author Florian Klein, florian.klein@hhu.de, 03.09.2013
     */
    static final class NodeEntry {

        // configuration values
        /**
         * Address and port of a DXRAM node
         */
        @Expose
        private IPV4Unit m_address = new IPV4Unit("127.0.0.1", 22222);
        /**
         * Role of the node (superpeer, peer, terminal)
         */
        @Expose
        private NodeRole m_role = NodeRole.PEER;
        /**
         * Rack id
         */
        @Expose
        private short m_rack = 0;
        /**
         * Switch id
         */
        @Expose
        private short m_switch = 0;
        /**
         * If 1, this entry is read from file, 0 if the node joined the system without being part of the initial configuration
         */
        @Expose
        private byte m_readFromFile = 1;

        /**
         * Creates an instance of NodesConfigurationEntry
         */
        NodeEntry() {
        }

        /**
         * Creates an instance of NodesConfigurationEntry
         *
         * @param p_address
         *     addres of the node
         * @param p_rack
         *     the rack of the node
         * @param p_switch
         *     the switcharea of the node
         * @param p_role
         *     the role of the node
         * @param p_readFromFile
         *     whether this node's information was read from nodes file or not
         */
        NodeEntry(final IPV4Unit p_address, final short p_rack, final short p_switch, final NodeRole p_role, final boolean p_readFromFile) {
            assert p_rack >= 0;
            assert p_switch >= 0;
            assert p_role != null;

            m_address = p_address;
            m_rack = p_rack;
            m_switch = p_switch;
            m_role = p_role;
            m_readFromFile = (byte) (p_readFromFile ? 1 : 0);
        }

        /**
         * Gets the address of the node
         *
         * @return the address of the node
         */
        public IPV4Unit getAddress() {
            return m_address;
        }

        /**
         * Gets the rack of the node
         *
         * @return the rack of the node
         */
        public short getRack() {
            return m_rack;
        }

        /**
         * Gets the switcharea of the node
         *
         * @return the switcharea of the node
         */
        public short getSwitch() {
            return m_switch;
        }

        /**
         * Gets the role of the node
         *
         * @return the role of the node
         */
        public NodeRole getRole() {
            return m_role;
        }

        @Override
        public String toString() {
            return "NodesConfigurationEntry [m_address=" + m_address + ", m_rack=" + m_rack + ", m_switch=" + m_switch + ", m_role=" + m_role.getAcronym() +
                ", m_readFromFile=" + (m_readFromFile == 1 ? "true" : "false") + ']';
        }

        /**
         * Gets the source of the node's information
         *
         * @return whether this node's information was read from nodes file or not
         */
        boolean readFromFile() {
            return m_readFromFile == 1;
        }

    }
}
