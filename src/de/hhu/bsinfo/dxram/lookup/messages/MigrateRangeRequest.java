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
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.utils.NodeID;

/**
 * Migrate Range Request
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 03.06.2013
 */
public class MigrateRangeRequest extends Request {

    // Attributes
    private long m_startChunkID;
    private long m_endChunkID;
    private short m_nodeID;
    private boolean m_isBackup;

    // Constructors

    /**
     * Creates an instance of MigrateRangeRequest
     */
    public MigrateRangeRequest() {
        super();

        m_startChunkID = ChunkID.INVALID_ID;
        m_endChunkID = ChunkID.INVALID_ID;
        m_nodeID = NodeID.INVALID_ID;
        m_isBackup = false;
    }

    /**
     * Creates an instance of MigrateRangeRequest
     *
     * @param p_destination
     *         the destination
     * @param p_startChunkID
     *         the first object that has to be migrated
     * @param p_endChunkID
     *         the last object that has to be migrated
     * @param p_nodeID
     *         the peer where the object has to be migrated
     * @param p_isBackup
     *         whether this is a backup message or not
     */
    public MigrateRangeRequest(final short p_destination, final long p_startChunkID, final long p_endChunkID, final short p_nodeID, final boolean p_isBackup) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_MIGRATE_RANGE_REQUEST);

        m_startChunkID = p_startChunkID;
        m_endChunkID = p_endChunkID;
        m_nodeID = p_nodeID;
        m_isBackup = p_isBackup;
    }

    // Getters

    /**
     * Get the first ChunkID
     *
     * @return the ID
     */
    public final long getStartChunkID() {
        return m_startChunkID;
    }

    /**
     * Get the last ChunkID
     *
     * @return the ID
     */
    public final long getEndChunkID() {
        return m_endChunkID;
    }

    /**
     * Get the NodeID
     *
     * @return the NodeID
     */
    public final short getNodeID() {
        return m_nodeID;
    }

    /**
     * Returns whether this is a backup message or not
     *
     * @return whether this is a backup message or not
     */
    public final boolean isBackup() {
        return m_isBackup;
    }

    @Override
    protected final int getPayloadLength() {
        return Long.BYTES * 2 + Short.BYTES + Byte.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeLong(m_startChunkID);
        p_exporter.writeLong(m_endChunkID);
        p_exporter.writeShort(m_nodeID);
        p_exporter.writeBoolean(m_isBackup);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_startChunkID = p_importer.readLong(m_startChunkID);
        m_endChunkID = p_importer.readLong(m_endChunkID);
        m_nodeID = p_importer.readShort(m_nodeID);
        m_isBackup = p_importer.readBoolean(m_isBackup);
    }

}
