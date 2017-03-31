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

package de.hhu.bsinfo.dxram.data;

import java.security.InvalidParameterException;

import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * Special chunk that allows get calls without knowing the chunk size
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.03.2017
 */
public final class ChunkAnon implements Importable, Exportable {
    private long m_id = ChunkID.INVALID_ID;
    private ChunkState m_state = ChunkState.OK;
    private byte[] m_data;

    /**
     * Constructor
     * Used when sending a remote request and waiting for the incoming data to be written to the object
     *
     * @param p_id
     *     The ID of the chunk
     */
    public ChunkAnon(final long p_id) {
        m_id = p_id;
        m_data = null;
    }

    /**
     * Constructor
     * Wrap an external byte array with this object
     *
     * @param p_id
     *     ID the chunk is assigned to.
     * @param p_buffer
     *     External buffer containing the data for the chunk. Be careful
     *     with shared references.
     */
    public ChunkAnon(final long p_id, final byte[] p_buffer) {
        m_id = p_id;

        if (p_buffer == null) {
            throw new InvalidParameterException("p_buffer == null");
        }

        m_data = p_buffer;
    }

    /**
     * Get the unique identifier of this chunk.
     *
     * @return The unique identifier (chunk id)
     */
    public long getID() {
        return m_id;
    }

    /**
     * Get the current state of the chunk. The state is set by DXRAM when execution DXRAM operations
     * on the chunk
     *
     * @return State set by the last DXRAM operation involving this chunk
     */
    public ChunkState getState() {
        return m_state;
    }

    /**
     * Set the state of the chunk. This is used by DXRAM on operations to indicate errors on operations.
     * Applications using DXRAM don't have to call this
     *
     * @param p_state
     *     State to set.
     */
    public void setState(final ChunkState p_state) {
        m_state = p_state;
    }

    /**
     * Gets the byte array containing the chunk's payload
     *
     * @return Byte array containing the chunk's payload
     */
    public byte[] getData() {
        return m_data;
    }

    /**
     * Gets the size of the data/payload.
     *
     * @return Payload size in bytes.
     */
    public final int getDataSize() {
        return m_data.length;
    }

    @Override
    public String toString() {
        return "Chunk [" + ChunkID.toHexString(m_id) + ", " + m_state + ", " + getDataSize() + ']';
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        // IMPORTANT: this writes length field AND data!
        p_exporter.writeByteArray(m_data);
    }

    @Override
    public void importObject(final Importer p_importer) {
        // IMPORTANT: this reads a full byte array WITH length field from the importer!
        m_data = p_importer.readByteArray();
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofByteArray(m_data);
    }
}
