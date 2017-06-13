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

package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.hhu.bsinfo.net.core.AbstractResponse;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Response to a JoinRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class JoinResponse extends AbstractResponse {

    // Attributes
    private short m_newContactSuperpeer;
    private short m_predecessor;
    private short m_successor;
    private ArrayList<Short> m_superpeers;
    private ArrayList<Short> m_peers;
    private byte[] m_metadata;

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
        m_metadata = null;
    }

    /**
     * Creates an instance of JoinResponse
     *
     * @param p_request
     *     the corresponding JoinRequest
     * @param p_newContactSuperpeer
     *     the superpeer that has to be asked next
     * @param p_predecessor
     *     the predecessor
     * @param p_successor
     *     the successor
     * @param p_superpeers
     *     the finger superpeers
     * @param p_peers
     *     the peers the superpeer is responsible for
     * @param p_metadata
     *     the metadata
     */
    public JoinResponse(final JoinRequest p_request, final short p_newContactSuperpeer, final short p_predecessor, final short p_successor,
        final ArrayList<Short> p_superpeers, final ArrayList<Short> p_peers, final byte[] p_metadata) {
        super(p_request, LookupMessages.SUBTYPE_JOIN_RESPONSE);

        m_newContactSuperpeer = p_newContactSuperpeer;
        m_predecessor = p_predecessor;
        m_successor = p_successor;
        m_superpeers = p_superpeers;
        m_peers = p_peers;
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
            ret = Byte.BYTES + Short.BYTES * 2;

            ret += Integer.BYTES;
            if (m_superpeers != null && !m_superpeers.isEmpty()) {
                ret += Short.BYTES * m_superpeers.size();
            }

            ret += Integer.BYTES;
            if (m_peers != null && !m_peers.isEmpty()) {
                ret += Short.BYTES * m_peers.size();
            }

            ret += Integer.BYTES;
            if (m_metadata != null && m_metadata.length > 0) {
                ret += m_metadata.length;
            }
        } else {
            ret = Byte.BYTES + Short.BYTES;
        }

        return ret;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        if (m_newContactSuperpeer == NodeID.INVALID_ID) {
            p_buffer.put((byte) 1);
            p_buffer.putShort(m_predecessor);
            p_buffer.putShort(m_successor);

            if (m_superpeers == null || m_superpeers.isEmpty()) {
                p_buffer.putInt(0);
            } else {
                p_buffer.putInt(m_superpeers.size());
                for (short superpeer : m_superpeers) {
                    p_buffer.putShort(superpeer);
                }
            }

            if (m_peers == null || m_peers.isEmpty()) {
                p_buffer.putInt(0);
            } else {
                p_buffer.putInt(m_peers.size());
                for (short peer : m_peers) {
                    p_buffer.putShort(peer);
                }
            }

            if (m_metadata == null || m_metadata.length == 0) {
                p_buffer.putInt(0);
            } else {
                p_buffer.putInt(m_metadata.length);
                p_buffer.put(m_metadata);
            }
        } else {
            p_buffer.put((byte) 0);
            p_buffer.putShort(m_newContactSuperpeer);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int length;

        if (p_buffer.get() != 0) {
            m_predecessor = p_buffer.getShort();
            m_successor = p_buffer.getShort();

            m_superpeers = new ArrayList<Short>();
            length = p_buffer.getInt();
            for (int i = 0; i < length; i++) {
                m_superpeers.add(p_buffer.getShort());
            }

            m_peers = new ArrayList<Short>();
            length = p_buffer.getInt();
            for (int i = 0; i < length; i++) {
                m_peers.add(p_buffer.getShort());
            }

            length = p_buffer.getInt();
            if (length != 0) {
                m_metadata = new byte[length];
                p_buffer.get(m_metadata);
            }
        } else {
            m_newContactSuperpeer = p_buffer.getShort();
        }
    }

}
