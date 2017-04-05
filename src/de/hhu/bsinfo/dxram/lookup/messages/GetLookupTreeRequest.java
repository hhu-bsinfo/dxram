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

/**
 * Get LookupTreeRequest
 *
 * @author Michael Birkhoff, michael.birkhoff@hhu.de, 06.09.2016
 */
public class GetLookupTreeRequest extends AbstractRequest {

    // Attributes
    private short m_nidToGetTreeFrom;

    // Constructors

    /**
     * Creates an instance of GetLookupTreeRequest
     */
    public GetLookupTreeRequest() {
        super();
    }

    /**
     * Creates an instance of GetLookupTreeRequest
     *
     * @param p_destination
     *     the destination
     * @param p_nidToGetTreeFrom
     *     the NodeID
     */
    public GetLookupTreeRequest(final short p_destination, final short p_nidToGetTreeFrom) {

        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_GET_LOOKUP_TREE_REQUEST);

        m_nidToGetTreeFrom = p_nidToGetTreeFrom;
    }

    /**
     * Returns the NodeID
     *
     * @return the NodeID
     */
    public short getTreeNodeID() {
        return m_nidToGetTreeFrom;
    }

    @Override
    protected final int getPayloadLength() {

        return Short.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {

        p_buffer.putShort(m_nidToGetTreeFrom);

    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {

        m_nidToGetTreeFrom = p_buffer.getShort();

    }

}
