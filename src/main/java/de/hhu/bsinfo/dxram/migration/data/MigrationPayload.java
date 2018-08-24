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

import de.hhu.bsinfo.dxram.migration.LongRange;
import de.hhu.bsinfo.dxutils.serialization.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MigrationPayload implements Importable, Exportable {

    private long[] m_ranges;

    private byte[][] m_data;

    private int m_chunkCount;

    public MigrationPayload() {

    }

    public MigrationPayload(List<LongRange> p_ranges, byte[][] p_data) {
        if ((m_chunkCount = LongRange.collectionToSize(p_ranges)) != p_data.length) {
            throw new IllegalArgumentException("Chunk id and data block count did not match");
        }

        m_ranges = LongRange.collectionToArray(p_ranges);

        m_data = p_data;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLongArray(m_ranges);

        p_exporter.writeInt(m_chunkCount);
        for (byte[] chunkData : m_data) {
            p_exporter.writeByteArray(chunkData);
        }
    }

    @Override
    public void importObject(Importer p_importer) {
        m_ranges = p_importer.readLongArray(m_ranges);

        m_chunkCount = p_importer.readInt(m_chunkCount);

        if (m_data == null) {
            m_data = new byte[m_chunkCount][];
        }

        for (int i = 0; i < m_data.length; i++) {
            m_data[i] = p_importer.readByteArray(m_data[i]);
        }
    }

    @Override
    public int sizeofObject() {
        int size = ObjectSizeUtil.sizeofLongArray(m_ranges);

        for (int i = 0; i < m_data.length; i++) {

            size += ObjectSizeUtil.sizeofByteArray(m_data[i]);
        }

        return size + Integer.BYTES;
    }

    public List<LongRange> getLongRanges() {
        List<LongRange> ranges = new ArrayList<>();

        for (int i = 0; i < m_ranges.length - 1; i += 2) {
            ranges.add(new LongRange(m_ranges[i], m_ranges[i + 1]));
        }

        return ranges;
    }

    public byte[][] getData() {
        return m_data;
    }

    public long[] getRanges() {
        return m_ranges;
    }

    public int getSize() {
        return Arrays.stream(m_data)
                .map(a -> a.length)
                .reduce(0, (a, b) -> a + b);
    }
}
