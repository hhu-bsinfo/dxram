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

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.NodeRegistry;
import de.hhu.bsinfo.dxram.boot.NodesConfiguration;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Join Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class JoinRequest extends Request {

    private byte[] m_nodeDetails;

    private NodeRegistry.NodeDetails m_cachedDetails = null;

    /**
     * Creates an instance of JoinRequest
     */
    public JoinRequest() {
        super();
    }

    /**
     * Creates an instance of JoinRequest.
     *
     *  @param p_destination The destination's node id.
     */
    public JoinRequest(final short p_destination, final NodeRegistry.NodeDetails p_details) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_JOIN_REQUEST);
        m_nodeDetails = p_details.toByteArray();
    }

    public NodeRegistry.NodeDetails getNodeDetails() {
        if (m_cachedDetails == null) {
            m_cachedDetails = NodeRegistry.NodeDetails.fromByteArray(m_nodeDetails);
        }

        return m_cachedDetails;
    }

    // Getters

    /**
     * Get new node
     *
     * @return the NodeID
     */
    public final short getNodeId() {
        return getNodeDetails().getId();
    }

    /**
     * Get role of new node
     *
     * @return true if the new node is a superpeer, false otherwise
     */
    public final boolean isSuperPeer() {
        return getNodeDetails().getRole() == NodeRole.SUPERPEER;
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofByteArray(m_nodeDetails);
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeByteArray(m_nodeDetails);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_nodeDetails = p_importer.readByteArray(m_nodeDetails);
    }
}
