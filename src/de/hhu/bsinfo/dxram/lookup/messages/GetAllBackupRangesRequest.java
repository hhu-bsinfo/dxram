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

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Get Backup Ranges Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 08.10.2015
 */
public class GetAllBackupRangesRequest extends Request {

    // Attributes
    private short m_nodeID;

    // Constructors

    /**
     * Creates an instance of GetBackupRangesRequest
     */
    public GetAllBackupRangesRequest() {
        super();

        m_nodeID = NodeID.INVALID_ID;
    }

    /**
     * Creates an instance of GetBackupRangesRequest
     *
     * @param p_destination
     *         the destination
     * @param p_nodeID
     *         the NodeID
     */
    public GetAllBackupRangesRequest(final short p_destination, final short p_nodeID) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_GET_ALL_BACKUP_RANGES_REQUEST);

        m_nodeID = p_nodeID;
    }

    // Getters

    /**
     * Get the NodeID
     *
     * @return the NodeID
     */
    public final short getNodeID() {
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
        m_nodeID = p_importer.readShort(m_nodeID);
    }

}
