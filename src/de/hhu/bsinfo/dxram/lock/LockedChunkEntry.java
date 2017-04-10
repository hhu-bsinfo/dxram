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

package de.hhu.bsinfo.dxram.lock;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Entry for a locked chunk
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 10.04.2017
 */
public class LockedChunkEntry implements Importable, Exportable {
    public static final int SIZEOF_OBJECT = Long.BYTES + Short.BYTES;

    private long m_chunkId;
    private short m_nodeId;

    /**
     * Default constructor for importing
     */
    public LockedChunkEntry() {
        m_chunkId = ChunkID.INVALID_ID;
        m_nodeId = NodeID.INVALID_ID;
    }

    /**
     * Constructor
     *
     * @param p_chunkId
     *     Locked chunk id
     * @param p_nodeId
     *     Peer that locked the chunk
     */
    public LockedChunkEntry(final long p_chunkId, final short p_nodeId) {
        m_chunkId = p_chunkId;
        m_nodeId = p_nodeId;
    }

    /**
     * Get the locked chunk id
     *
     * @return Chunk id locked
     */
    public long getChunkId() {
        return m_chunkId;
    }

    /**
     * Get the peer that locked the chunk
     *
     * @return Node id of the peer that locked the chunk
     */
    public short getNodeId() {
        return m_nodeId;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(m_chunkId);
        p_exporter.writeShort(m_nodeId);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_chunkId = p_importer.readLong();
        m_nodeId = p_importer.readShort();
    }

    @Override
    public int sizeofObject() {
        return SIZEOF_OBJECT;
    }
}
