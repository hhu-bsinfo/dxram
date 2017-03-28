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

package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Index data chunk for the nameservice.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class NameServiceIndexData extends DataStructure {
    private static final int MS_NUM_INDICES = 10000;

    private short m_numEntries;
    private int[] m_keys = new int[MS_NUM_INDICES];
    private long[] m_chunkIDs = new long[MS_NUM_INDICES];
    private long m_nextIndexDataChunkId = ChunkID.INVALID_ID;

    /**
     * Default constructor
     */
    public NameServiceIndexData() {

    }

    /**
     * Chain multiple indices for expansion creating a linked list.
     *
     * @param p_chunkID
     *     ChunkID of the next data index to chain to this one.
     */
    public void setNextIndexDataChunk(final long p_chunkID) {
        m_nextIndexDataChunkId = p_chunkID;
    }

    /**
     * Insert a new mapping into the index.
     *
     * @param p_key
     *     Key of the mapping.
     * @param p_chunkId
     *     Chunk id to map to the key.
     * @return True if adding successful, false if index is full.
     */
    public boolean insertMapping(final int p_key, final long p_chunkId) {
        if (m_numEntries == MS_NUM_INDICES) {
            return false;
        }

        m_keys[m_numEntries] = p_key;
        m_chunkIDs[m_numEntries] = p_chunkId;
        m_numEntries++;
        return true;
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_numEntries = p_importer.readShort();
        for (int i = 0; i < MS_NUM_INDICES; i++) {
            m_keys[i] = p_importer.readInt();
            m_chunkIDs[i] = p_importer.readLong();
        }
        m_nextIndexDataChunkId = p_importer.readLong();
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeShort(m_numEntries);
        for (int i = 0; i < MS_NUM_INDICES; i++) {
            p_exporter.writeInt(m_keys[i]);
            p_exporter.writeLong(m_chunkIDs[i]);
        }
        p_exporter.writeLong(m_nextIndexDataChunkId);
    }

    @Override
    public int sizeofObject() {
        return Short.BYTES + Integer.BYTES * MS_NUM_INDICES + Long.BYTES * MS_NUM_INDICES + Long.BYTES;
    }
}
