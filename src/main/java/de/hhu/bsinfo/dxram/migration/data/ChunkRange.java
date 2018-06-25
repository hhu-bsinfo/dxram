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

package de.hhu.bsinfo.dxram.migration.data;

import de.hhu.bsinfo.dxutils.serialization.*;

public class ChunkRange implements Importable, Exportable {

    private long m_startId;

    private long m_endId;

    private byte[][] m_data;

    private int m_size;

    public ChunkRange() {

    }

    public ChunkRange(long p_startId, long p_endId, byte[][] p_data) {

        if ( (p_endId - p_startId + 1) != p_data.length) {

            throw new IllegalArgumentException("Chunk id and data block count did not match");
        }

        m_startId = p_startId;

        m_endId = p_endId;

        m_data = p_data;
    }

    @Override
    public void exportObject(Exporter p_exporter) {

        p_exporter.writeLong(m_startId);

        p_exporter.writeLong(m_endId);

        for (int i = 0; i < m_data.length; i++) {

            p_exporter.writeByteArray(m_data[i]);
        }
    }

    @Override
    public void importObject(Importer p_importer) {

        m_startId = p_importer.readLong(m_startId);

        m_endId = p_importer.readLong(m_endId);

        if (m_data == null) {

            m_data = new byte[size()][];
        }

        // TODO(krakowski)
        //  Switch to readByteArray() once it works
        for (int i = 0; i < m_data.length; i++) {

//            m_size = p_importer.readCompactNumber(m_size);
//
//            if (m_data[i] == null) {
//
//                m_data[i] = new byte[m_size];
//            }
//
//            p_importer.readBytes(m_data[i]);

            m_data[i] = p_importer.readByteArray(m_data[i]);
        }
    }

    @Override
    public int sizeofObject() {

        int size = Long.BYTES * 2;

        for (int i = 0; i < m_data.length; i++) {

            size += ObjectSizeUtil.sizeofByteArray(m_data[i]);
        }

        return size;
    }

    public int size() {

        return (int) (m_endId - m_startId + 1);
    }

    public byte[][] getData() {
        return m_data;
    }

    public long getStartId() {
        return m_startId;
    }

    public long getEndId() {
        return m_endId;
    }
}
