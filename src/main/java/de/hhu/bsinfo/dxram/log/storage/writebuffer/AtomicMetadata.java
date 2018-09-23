/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating
 * Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.log.storage.writebuffer;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * This class is used to return the metadata required for flushing the write buffer.
 * The metadata is collected in a synchronized area.
 * There is only one instance of this class as the process thread is the only user.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 15.09.2018
 */
final class AtomicMetadata {
    private static final AtomicMetadata INSTANCE = new AtomicMetadata();

    private ByteBuffer m_writeBuffer;
    private int m_length;
    private List<int[]> m_lengthByBackupRange;

    /**
     * Private constructor.
     */
    private AtomicMetadata() {
    }

    /**
     * Sets the attributes and returns the instance.
     *
     * @param p_writeBuffer
     *         the byte buffer to access the write buffer
     * @param p_length
     *         the total number of bytes to flush
     * @param p_lengthByBackupRange
     *         the number of bytes per backup range
     * @return the instance
     */
    static AtomicMetadata getInstance(final ByteBuffer p_writeBuffer, final int p_length,
            final List<int[]> p_lengthByBackupRange) {
        INSTANCE.m_writeBuffer = p_writeBuffer;
        INSTANCE.m_length = p_length;
        INSTANCE.m_lengthByBackupRange = p_lengthByBackupRange;

        return INSTANCE;
    }

    /**
     * Returns the byte buffer.
     *
     * @return the byte buffer
     */
    ByteBuffer getByteBuffer() {
        return m_writeBuffer;
    }

    /**
     * Returns the total number of bytes to flush.
     *
     * @return the total length
     */
    int getTotalLength() {
        return m_length;
    }

    /**
     * Returns the number of bytes per backup range.
     *
     * @return the lengths
     */
    List<int[]> getAllLengths() {
        return m_lengthByBackupRange;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder =
                new StringBuilder("AtomicMetadata: ").append(m_writeBuffer).append("; ").append(m_length).append(';');

        for (int[] array : m_lengthByBackupRange) {
            stringBuilder.append(" [0x").append(Integer.toHexString(array[0]).toUpperCase()).append(',')
                    .append(array[1]).append(']');
        }

        return stringBuilder.toString();
    }
}
