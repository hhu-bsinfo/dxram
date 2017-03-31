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

import java.nio.ByteBuffer;
import java.security.InvalidParameterException;

import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Implementation of a data structure based on a ByteBuffer
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.03.2017
 */
public final class DSByteBuffer extends DataStructure {
    private ByteBuffer m_data;

    /**
     * Constructor
     * Sets the chunk id to invalid.
     *
     * @param p_bufferSize
     *     p_bufferSize Initial size of the byte buffer.
     */
    public DSByteBuffer(final int p_bufferSize) {
        super();
        m_data = ByteBuffer.allocate(p_bufferSize);
    }

    /**
     * Constructor
     *
     * @param p_id
     *     ID the chunk is assigned to.
     * @param p_bufferSize
     *     Initial size of the byte buffer.
     */
    public DSByteBuffer(final long p_id, final int p_bufferSize) {
        super(p_id);
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
    public DSByteBuffer(final ByteBuffer p_buffer) {
        super();

        if (p_buffer == null) {
            throw new InvalidParameterException("p_buffer == null");
        }

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
    public DSByteBuffer(final long p_id, final ByteBuffer p_buffer) {
        super(p_id);

        if (p_buffer == null) {
            throw new InvalidParameterException("p_buffer == null");
        }

        m_data = p_buffer;
    }

    /**
     * Gets the underlying byte buffer with the stored payload.
     *
     * @return ByteBuffer with position reseted.
     * @note The position gets reseted to 0 before returning the reference.
     */
    public final ByteBuffer getData() {
        m_data.position(0);
        return m_data;
    }

    @Override
    public String toString() {
        return "Chunk [" + ChunkID.toHexString(getID()) + ", " + getState() + ", " + m_data.capacity() + ']';
    }

    @Override
    public void importObject(final Importer p_importer) {
        p_importer.readBytes(m_data.array());
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeBytes(m_data.array(), 0, m_data.limit());
    }

    @Override
    public int sizeofObject() {
        return m_data.limit();
    }
}
