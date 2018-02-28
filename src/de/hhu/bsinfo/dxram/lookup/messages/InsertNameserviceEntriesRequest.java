/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

/**
 * Request for storing an id to ChunkID mapping on a remote node
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 */
public class InsertNameserviceEntriesRequest extends Request {

    // Attributes
    private int m_id;
    private long m_chunkID;
    private boolean m_isBackup;

    // Constructors

    /**
     * Creates an instance of InsertIDRequest
     */
    public InsertNameserviceEntriesRequest() {
        super();

        m_id = -1;
        m_chunkID = ChunkID.INVALID_ID;
        m_isBackup = false;
    }

    /**
     * Creates an instance of InsertIDRequest
     *
     * @param p_destination
     *         the destination
     * @param p_id
     *         the id to store
     * @param p_chunkID
     *         the ChunkID to store
     * @param p_isBackup
     *         whether this is a backup message or not
     */
    public InsertNameserviceEntriesRequest(final short p_destination, final int p_id, final long p_chunkID, final boolean p_isBackup) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_INSERT_NAMESERVICE_ENTRIES_REQUEST);

        m_id = p_id;
        m_chunkID = p_chunkID;
        m_isBackup = p_isBackup;
    }

    // Getters

    /**
     * Get the id to store
     *
     * @return the id to store
     */
    public final int getID() {
        return m_id;
    }

    /**
     * Get the ChunkID to store
     *
     * @return the ChunkID to store
     */
    public final long getChunkID() {
        return m_chunkID;
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
        return Integer.BYTES + Long.BYTES + Byte.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_id);
        p_exporter.writeLong(m_chunkID);
        p_exporter.writeBoolean(m_isBackup);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_id = p_importer.readInt(m_id);
        m_chunkID = p_importer.readLong(m_chunkID);
        m_isBackup = p_importer.readBoolean(m_isBackup);
    }

}
