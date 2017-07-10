/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.net.core.AbstractRequest;

/**
 * Peer Join Event Request. Request to propagate a peer joining to all other peers (two-phase: 1. inform all superpeers 2. superpeers inform peers).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 03.04.2017
 */
public class PeerJoinEventRequest extends AbstractRequest {

    // Attributes
    private short m_nodeID;

    // Constructors

    /**
     * Creates an instance of PeerJoinEventRequest
     */
    public PeerJoinEventRequest() {
        super();
    }

    /**
     * Creates an instance of PeerJoinEventRequest
     *
     * @param p_destination
     *         the destination
     * @param p_nodeID
     *         the NodeID of the joined peer
     */
    public PeerJoinEventRequest(final short p_destination, final short p_nodeID) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_PEER_JOIN_EVENT_REQUEST);

        m_nodeID = p_nodeID;
    }

    // Getters

    /**
     * Get the joined peer
     *
     * @return the NodeID
     */
    public final short getJoinedPeer() {
        return m_nodeID;
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_nodeID);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_nodeID = p_importer.readShort();
    }

}
