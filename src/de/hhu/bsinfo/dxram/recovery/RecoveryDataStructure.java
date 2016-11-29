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

package de.hhu.bsinfo.dxram.recovery;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Object to hand over recovered data to memory management.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 08.11.2016
 */
public class RecoveryDataStructure implements DataStructure {
    private long m_id = ChunkID.INVALID_ID;

    private byte[] m_data;
    private int m_offset;
    private int m_length;

    /**
     * Constructor
     */
    public RecoveryDataStructure() {

    }

    /**
     * Constructor
     *
     * @param p_id
     *     Chunk ID to assign.
     */
    public RecoveryDataStructure(final byte[] p_data) {
        m_data = p_data;
        m_offset = 0;
        m_length = 0;
    }

    // -----------------------------------------------------------------------------

    @Override
    public long getID() {
        return m_id;
    }

    @Override
    public void setID(long p_id) {
        m_id = p_id;
    }

    /**
     * Set the id of the target vertex.
     *
     * @param p_id
     *     Id of the target vertex to set.
     */
    public int getLength() {
        return m_length;
    }

    /**
     * Set the id of the target vertex.
     *
     * @param p_id
     *     Id of the target vertex to set.
     */
    public void setLength(final int p_length) {
        m_length = p_length;
    }

    /**
     * Set the id of the source vertex.
     *
     * @param p_id
     *     Id of the source vertex to set.
     */
    public void setOffset(final int p_offset) {
        m_offset = p_offset;
    }

    // -----------------------------------------------------------------------------

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeBytes(m_data, m_offset, m_length);
    }

    @Override
    public void importObject(final Importer p_importer) {
        // unused
    }

    @Override
    public int sizeofObject() {
        return m_length;
    }
}
