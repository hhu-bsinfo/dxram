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

package de.hhu.bsinfo.dxram.migration.messages;

import de.hhu.bsinfo.dxutils.serialization.*;

public class ChunkCollection implements Importable, Exportable {

    private long[] m_chunkIds;

    private byte[][] m_data;

    public ChunkCollection(long[] p_chunkIds, byte[][] p_data) {

        if (p_chunkIds.length != p_data.length) {

            throw new IllegalArgumentException("Chunk id and data block count did not match");
        }

        m_chunkIds = p_chunkIds;

        m_data = p_data;
    }

    @Override
    public void exportObject(Exporter p_exporter) {

        p_exporter.writeLongArray(m_chunkIds);

        for (int i = 0; i < m_data.length; i++) {

            p_exporter.writeByteArray(m_data[i]);
        }
    }

    @Override
    public void importObject(Importer p_importer) {

        m_chunkIds = p_importer.readLongArray(m_chunkIds);

        m_data = new byte[m_chunkIds.length][];

        for (int i = 0; i < m_data.length; i++) {

            m_data[i] = p_importer.readByteArray(m_data[i]);
        }
    }

    @Override
    public int sizeofObject() {

        int size = ObjectSizeUtil.sizeofLongArray(m_chunkIds);

        for (int i = 0; i < m_data.length; i++) {

            size += ObjectSizeUtil.sizeofByteArray(m_data[i]);
        }

        return size;
    }

    public int size() {

        return m_chunkIds.length;
    }

    public byte[][] getData() {
        return m_data;
    }

    public long[] getChunkIds() {
        return m_chunkIds;
    }
}
