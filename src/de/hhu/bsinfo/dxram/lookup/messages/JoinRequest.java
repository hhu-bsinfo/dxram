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

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Join Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class JoinRequest extends AbstractRequest {

    // Attributes
    private short m_newNode;
    private boolean m_nodeIsSuperpeer;

    // Constructors

    /**
     * Creates an instance of JoinRequest
     */
    public JoinRequest() {
        super();

        m_newNode = NodeID.INVALID_ID;
        m_nodeIsSuperpeer = false;
    }

    /**
     * Creates an instance of JoinRequest
     *
     * @param p_destination
     *     the destination
     * @param p_newNode
     *     the NodeID of the new node
     * @param p_nodeIsSuperpeer
     *     wether the new node is a superpeer or not
     */
    public JoinRequest(final short p_destination, final short p_newNode, final boolean p_nodeIsSuperpeer) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_JOIN_REQUEST);

        assert p_newNode != 0;

        m_newNode = p_newNode;
        m_nodeIsSuperpeer = p_nodeIsSuperpeer;
    }

    // Getters

    /**
     * Get new node
     *
     * @return the NodeID
     */
    public final short getNewNode() {
        return m_newNode;
    }

    /**
     * Get role of new node
     *
     * @return true if the new node is a superpeer, false otherwise
     */
    public final boolean nodeIsSuperpeer() {
        return m_nodeIsSuperpeer;
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES + Byte.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putShort(m_newNode);

        if (m_nodeIsSuperpeer) {
            p_buffer.put((byte) 1);
        } else {
            p_buffer.put((byte) 0);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_newNode = p_buffer.getShort();

        final byte b = p_buffer.get();
        if (b == 1) {
            m_nodeIsSuperpeer = true;
        }
    }

}
