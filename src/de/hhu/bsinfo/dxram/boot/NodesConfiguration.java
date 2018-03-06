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

import java.util.ArrayList;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import de.hhu.bsinfo.dxutils.unit.IPV4Unit;

/**
 * Represents a nodes configuration for DXRAM. This also holds any information
 * about the current node as well as any remote nodes available in the system.
 *
 * @author Florian Klein, florian.klein@hhu.de, 03.09.2013
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 9.12.2015
 */
public final class NodesConfiguration {

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
     *         Node id to set.
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
        StringBuilder str = new StringBuilder();

        str.append("NodesConfiguration[ownID: ").append(m_ownID).append("]:");
        for (int i = 0; i < m_nodes.length; i++) {
            if (m_nodes[i] != null) {
                str.append('\n').append(NodeID.toHexString((short) i)).append(": ").append(m_nodes[i]);
            }
        }

        return str.toString();
    }

    /**
     * Get the NodeEntry of the specified node ID.
     *
     * @param p_nodeID
     *         Node ID to get the entry of.
     * @return NodeEntry containing information about the node or null if it does not exist.
     */
    NodeEntry getNode(final short p_nodeID) {
        return m_nodes[p_nodeID & 0xFFFF];
    }

    /**
     * Get all online nodes.
     *
     * @return all NodeEntries from online nodes.
     */
    ArrayList<NodeEntry> getOnlineNodes() {
        NodeEntry entry;
        ArrayList<NodeEntry> list = new ArrayList<>();
        for (int i = 0; i < m_nodes.length; i++) {
            entry = m_nodes[i];
            if (entry != null && entry.getStatus()) {
                list.add(entry);
            }
        }

        return list;
    }

    /**
     * Adds a node
     *
     * @param p_entry
     *         the configured node
     * @return whether this is a new entry or not
     */
    synchronized boolean addNode(final NodeEntry p_entry) {
        short nodeID = p_entry.getNodeID();
        NodeEntry prev = m_nodes[nodeID & 0xFFFF];

        m_nodes[nodeID & 0xFFFF] = p_entry;

        return prev == null || !prev.getAddress().equals(p_entry.getAddress());
    }

    /**
     * Remove a node from the mappings list.
     *
     * @param p_nodeID
     *         Node ID of the entry to remove.
     */
    synchronized void removeNode(final short p_nodeID) {
        m_nodes[p_nodeID & 0xFFFF] = null;
    }

    /**
     * Describes a nodes configuration entry
     *
     * @author Florian Klein, florian.klein@hhu.de, 03.09.2013
     */
    public static final class NodeEntry implements Importable, Exportable {

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

        private short m_nodeID = NodeID.INVALID_ID;
        private boolean m_online = false;
        private boolean m_availableForBackup = true;

        // Tmp. state for import
        private String m_addrStr;
        private short m_acr;

        /**
         * Creates an instance of NodesConfigurationEntry
         */
        public NodeEntry(final boolean p_isOnline) {
            m_online = p_isOnline;
        }

        /**
         * Creates an instance of NodesConfigurationEntry
         *
         * @param p_address
         *         addres of the node
         * @param p_rack
         *         the rack of the node
         * @param p_switch
         *         the switcharea of the node
         * @param p_role
         *         the role of the node
         * @param p_readFromFile
         *         whether this node's information was read from nodes file or not
         * @param p_availableForBackup
         *         whether this peer is available for backup/logging or not
         * @param p_isOnline
         *         true, if this node is online
         */
        NodeEntry(final IPV4Unit p_address, final short p_nodeID, final short p_rack, final short p_switch, final NodeRole p_role, final boolean p_readFromFile,
                final boolean p_availableForBackup, final boolean p_isOnline) {
            assert p_rack >= 0;
            assert p_switch >= 0;
            assert p_role != null;

            m_address = p_address;
            m_nodeID = p_nodeID;
            m_rack = p_rack;
            m_switch = p_switch;
            m_role = p_role;
            m_readFromFile = (byte) (p_readFromFile ? 1 : 0);
            m_availableForBackup = p_availableForBackup;
            m_online = p_isOnline;
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
         * Gets the NodeID
         *
         * @return the NodeID
         */
        public short getNodeID() {
            return m_nodeID;
        }

        /**
         * Sets the NodeID
         *
         * @param p_nodeID
         *         the NodeID
         */
        public void setNodeID(final short p_nodeID) {
            m_nodeID = p_nodeID;
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
            return "NodesConfigurationEntry [m_address=" + m_address + ", m_nodeID=" + m_nodeID + ", m_rack=" + m_rack + ", m_switch=" + m_switch +
                    ", m_role=" + m_role.getAcronym() + ", m_online=" + m_online + ", m_availableForBackup=" + m_availableForBackup + ", m_readFromFile=" +
                    (m_readFromFile == 1 ? "true" : "false") + ']';
        }

        /**
         * Gets the source of the node's information
         *
         * @return whether this node's information was read from nodes file or not
         */
        boolean readFromFile() {
            return m_readFromFile == 1;
        }

        /**
         * Returns whether this node is available for backup or not
         *
         * @return true, if node is available for backup
         */
        boolean isAvailableForBackup() {
            return m_availableForBackup;
        }

        /**
         * Returns whether this node is online or not
         *
         * @return true, if node is not available anymore
         */
        boolean getStatus() {
            return m_online;
        }

        /**
         * Marks this node as online/offline
         */
        void setStatus(final boolean p_online) {
            m_online = p_online;
        }

        @Override
        public void exportObject(Exporter p_exporter) {
            p_exporter.writeString(m_address.getAddressStr());
            p_exporter.writeShort(m_nodeID);
            p_exporter.writeShort((short) m_role.getAcronym());
            p_exporter.writeShort(m_rack);
            p_exporter.writeShort(m_switch);
            p_exporter.writeBoolean(m_availableForBackup);
        }

        @Override
        public void importObject(Importer p_importer) {
            m_addrStr = p_importer.readString(m_addrStr);
            String[] splitAddr = m_addrStr.split(":");
            m_address = new IPV4Unit(splitAddr[0], Integer.parseInt(splitAddr[1]));
            m_nodeID = p_importer.readShort(m_nodeID);
            m_acr = p_importer.readShort(m_acr);
            m_role = NodeRole.getRoleByAcronym((char) m_acr);
            m_rack = p_importer.readShort(m_rack);
            m_switch = p_importer.readShort(m_switch);
            m_availableForBackup = p_importer.readBoolean(m_availableForBackup);
        }

        @Override
        public int sizeofObject() {
            return ObjectSizeUtil.sizeofString(m_address.getAddressStr()) + 4 * Short.BYTES + Byte.BYTES;
        }
    }
}
