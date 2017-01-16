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

package de.hhu.bsinfo.dxram.data;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Default/generic implementation of a DataStructure. This can be used if the there is no
 * need to further specify the data to be stored as a chunk (i.e. a byte buffer is fine for the job).
 * Furthermore this class is used internally when chunks are moved between different nodes. The actual
 * structure is unknown and not relevant for these tasks, as we just want to work with the payload as
 * one package.
 * If a chunk is requested from the ChunkService, the internal buffer will be adjusted to the
 * actual size of the stored chunk in memory.
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class Chunk implements DataStructure {

    private long m_chunkID = ChunkID.INVALID_ID;
    private ByteBuffer m_data;

    /**
     * Constructor
     * Sets the chunk id to invalid.
     *
     * @param p_bufferSize
     *     p_bufferSize Initial size of the byte buffer. If unknown/to read the complete payload
     *     stored for the specified ID, you can set this to 0. The importObject function will
     *     allocate the exact size this chunk occupies in memory.
     */
    public Chunk(final int p_bufferSize) {
        m_chunkID = ChunkID.INVALID_ID;
        m_data = ByteBuffer.allocate(p_bufferSize);
    }

    /**
     * Constructor
     *
     * @param p_id
     *     ID the chunk is assigned to.
     * @param p_bufferSize
     *     Initial size of the byte buffer. If unknown/to read the complete payload
     *     stored for the specified ID, you can set this to 0. The importObject function will
     *     allocate the exact size this chunk occupies in memory.
     */
    public Chunk(final long p_id, final int p_bufferSize) {
        m_chunkID = p_id;
        m_data = ByteBuffer.allocate(p_bufferSize);
    }

    /**
     * Constructor
     * Sets the chunk id to invalid.
     * Create the chunk with an external buffer.
     *
     * @param p_buffer
     *     External buffer containing the data for the chunk. Be careful
     *     with shared references of the ByteBuffer object.
     */
    public Chunk(final ByteBuffer p_buffer) {
        m_chunkID = ChunkID.INVALID_ID;
        m_data = p_buffer;
    }

    /**
     * Constructor
     * Create the chunk with an external buffer.
     *
     * @param p_id
     *     ID the chunk is assigned to.
     * @param p_buffer
     *     External buffer containing the data for the chunk. Be careful
     *     with shared references of the ByteBuffer object.
     */
    public Chunk(final long p_id, final ByteBuffer p_buffer) {
        m_chunkID = p_id;
        m_data = p_buffer;
    }

    /**
     * Constructor
     * Create the chunk with chunk id, without data.
     *
     * @param p_id
     *     ID the chunk is assigned to.
     */
    public Chunk(final long p_id) {
        m_chunkID = p_id;
    }

    /**
     * Constructor
     * Create an empty chunk. Used for pooling.
     */
    public Chunk() {
        m_chunkID = ChunkID.INVALID_ID;
    }

    /**
     * Gets the underlying byte buffer with the stored payload.
     *
     * @return ByteBuffer with position reseted.
     * @note The position gets reseted to 0 before returning the reference.
     */
    public final ByteBuffer getData() {
        if (m_data == null) {
            return null;
        } else {
            m_data.position(0);
            return m_data;
        }
    }

    /**
     * Gets the size of the data/payload.
     *
     * @return Payload size in bytes.
     */
    public final int getDataSize() {
        if (m_data == null) {
            return 0;
        } else {
            return m_data.capacity();
        }
    }

    @Override
    public long getID() {
        return m_chunkID;
    }

    /**
     * Change the ID of this chunk. This can be used to re-use pre-allocated chunks (pooling).
     *
     * @param p_id
     *     New ID to set for this chunk.
     */
    @Override
    public void setID(final long p_id) {
        m_chunkID = p_id;
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + '[' + ChunkID.toHexString(m_chunkID) + ", " + getDataSize() + ']';
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_data = ByteBuffer.wrap(p_importer.readByteArray());
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        if (m_data != null) {
            p_exporter.writeByteArray(m_data.array());
        } else {
            p_exporter.writeByteArray(new byte[0]);
        }
    }

    @Override
    public int sizeofObject() {
        return getDataSize();
    }
}
