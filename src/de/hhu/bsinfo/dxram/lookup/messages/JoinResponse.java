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

package de.hhu.bsinfo.dxram.lookup.messages;

import java.util.ArrayList;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxram.boot.NodesConfiguration;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Response to a JoinRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class JoinResponse extends Response {

    // Attributes
    private short m_newContactSuperpeer;
    private short m_predecessor;
    private short m_successor;
    private ArrayList<Short> m_superpeers;
    private ArrayList<Short> m_peers;
    private ArrayList<NodesConfiguration.NodeEntry> m_onlineNodes;
    private byte[] m_metadata;

    private int m_superpeersToRead; // Used for serialization, only
    private int m_peersToRead; // Used for serialization, only
    private int m_nodesToRead; // Used for serialization, only

    // Constructors

    /**
     * Creates an instance of JoinResponse
     */
    public JoinResponse() {
        super();

        m_newContactSuperpeer = NodeID.INVALID_ID;
        m_predecessor = NodeID.INVALID_ID;
        m_successor = NodeID.INVALID_ID;
        m_superpeers = null;
        m_peers = null;
        m_onlineNodes = null;
        m_metadata = null;
    }

    /**
     * Creates an instance of JoinResponse
     *
     * @param p_request
     *         the corresponding JoinRequest
     * @param p_newContactSuperpeer
     *         the superpeer that has to be asked next
     * @param p_predecessor
     *         the predecessor
     * @param p_successor
     *         the successor
     * @param p_superpeers
     *         the finger superpeers
     * @param p_peers
     *         the peers the superpeer is responsible for
     * @param p_onlineNodes
     *         all available nodes with address, role, ...
     * @param p_metadata
     *         the metadata
     */
    public JoinResponse(final JoinRequest p_request, final short p_newContactSuperpeer, final short p_predecessor, final short p_successor,
            final ArrayList<Short> p_superpeers, final ArrayList<Short> p_peers, final ArrayList<NodesConfiguration.NodeEntry> p_onlineNodes,
            final byte[] p_metadata) {
        super(p_request, LookupMessages.SUBTYPE_JOIN_RESPONSE);

        m_newContactSuperpeer = p_newContactSuperpeer;
        m_predecessor = p_predecessor;
        m_successor = p_successor;
        m_superpeers = p_superpeers;
        m_peers = p_peers;
        m_onlineNodes = p_onlineNodes;
        m_metadata = p_metadata;
    }

    // Getters

    /**
     * Get new contact superpeer
     *
     * @return the NodeID
     */
    public final short getNewContactSuperpeer() {
        return m_newContactSuperpeer;
    }

    /**
     * Get predecessor
     *
     * @return the NodeID
     */
    public final short getPredecessor() {
        return m_predecessor;
    }

    /**
     * Get successor
     *
     * @return the NodeID
     */
    public final short getSuccessor() {
        return m_successor;
    }

    /**
     * Get superpeers
     *
     * @return the NodeIDs
     */
    public final ArrayList<Short> getSuperpeers() {
        return m_superpeers;
    }

    /**
     * Get peers
     *
     * @return the NodeIDs
     */
    public final ArrayList<Short> getPeers() {
        return m_peers;
    }

    /**
     * Get online nodes
     *
     * @return the nodes
     */
    public final ArrayList<NodesConfiguration.NodeEntry> getOnlineNodes() {
        return m_onlineNodes;
    }

    /**
     * Get metadata
     *
     * @return the byte array
     */
    public final byte[] getMetadata() {
        return m_metadata;
    }

    @Override
    protected final int getPayloadLength() {
        int ret;

        if (m_newContactSuperpeer == NodeID.INVALID_ID) {
            ret = Short.BYTES * 3;

            if (m_superpeers != null && !m_superpeers.isEmpty()) {
                ret += ObjectSizeUtil.sizeofCompactedNumber(m_superpeers.size()) + Short.BYTES * m_superpeers.size();
            } else {
                ret += Byte.BYTES;
            }

            if (m_peers != null && !m_peers.isEmpty()) {
                ret += ObjectSizeUtil.sizeofCompactedNumber(m_peers.size()) + Short.BYTES * m_peers.size();
            } else {
                ret += Byte.BYTES;
            }

            if (m_onlineNodes != null && !m_onlineNodes.isEmpty()) {
                ret += ObjectSizeUtil.sizeofCompactedNumber(m_onlineNodes.size());
                for (NodesConfiguration.NodeEntry entry : m_onlineNodes) {
                    ret += entry.sizeofObject();
                }
            } else {
                ret += Byte.BYTES;
            }

            if (m_metadata != null && m_metadata.length > 0) {
                ret += ObjectSizeUtil.sizeofByteArray(m_metadata);
            } else {
                ret += Byte.BYTES;
            }
        } else {
            ret = Short.BYTES;
        }

        return ret;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        if (m_newContactSuperpeer == NodeID.INVALID_ID) {
            p_exporter.writeShort(NodeID.INVALID_ID);
            p_exporter.writeShort(m_predecessor);
            p_exporter.writeShort(m_successor);

            if (m_superpeers == null || m_superpeers.isEmpty()) {
                p_exporter.writeCompactNumber(0);
            } else {
                p_exporter.writeCompactNumber(m_superpeers.size());
                for (short superpeer : m_superpeers) {
                    p_exporter.writeShort(superpeer);
                }
            }

            if (m_peers == null || m_peers.isEmpty()) {
                p_exporter.writeCompactNumber(0);
            } else {
                p_exporter.writeCompactNumber(m_peers.size());
                for (short peer : m_peers) {
                    p_exporter.writeShort(peer);
                }
            }

            if (m_onlineNodes == null || m_onlineNodes.isEmpty()) {
                p_exporter.writeCompactNumber(0);
            } else {
                p_exporter.writeCompactNumber(m_onlineNodes.size());
                for (NodesConfiguration.NodeEntry entry : m_onlineNodes) {
                    p_exporter.exportObject(entry);
                }
            }

            if (m_metadata == null || m_metadata.length == 0) {
                p_exporter.writeCompactNumber(0);
            } else {
                p_exporter.writeByteArray(m_metadata);
            }
        } else {
            p_exporter.writeShort(m_newContactSuperpeer);
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_newContactSuperpeer = p_importer.readShort(m_newContactSuperpeer);
        if (m_newContactSuperpeer == NodeID.INVALID_ID) {
            m_predecessor = p_importer.readShort(m_predecessor);
            m_successor = p_importer.readShort(m_successor);

            m_superpeersToRead = p_importer.readCompactNumber(m_superpeersToRead);
            if (m_superpeers == null) {
                // Do not overwrite existing array list
                m_superpeers = new ArrayList<Short>(m_superpeersToRead);
            }
            for (int i = 0; i < m_superpeersToRead; i++) {
                short superpeer = p_importer.readShort((short) 0);
                if (m_superpeers.size() == i) {
                    m_superpeers.add(superpeer);
                }
            }

            m_peersToRead = p_importer.readCompactNumber(m_peersToRead);
            if (m_peers == null) {
                // Do not overwrite existing array list
                m_peers = new ArrayList<Short>(m_peersToRead);
            }
            for (int i = 0; i < m_peersToRead; i++) {
                short peer = p_importer.readShort((short) 0);
                if (m_peers.size() == i) {
                    m_peers.add(peer);
                }
            }

            m_nodesToRead = p_importer.readCompactNumber(m_nodesToRead);
            if (m_onlineNodes == null) {
                // Do not overwrite existing array list
                m_onlineNodes = new ArrayList<NodesConfiguration.NodeEntry>(m_nodesToRead);
            }
            for (int i = 0; i < m_nodesToRead; i++) {
                NodesConfiguration.NodeEntry node = new NodesConfiguration.NodeEntry(true);
                p_importer.importObject(node);

                if (m_onlineNodes.size() == i) {
                    m_onlineNodes.add(node);
                }
            }

            m_metadata = p_importer.readByteArray(m_metadata);
        }
    }

}
