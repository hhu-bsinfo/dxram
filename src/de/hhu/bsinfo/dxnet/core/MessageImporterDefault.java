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

package de.hhu.bsinfo.dxnet.core;

import de.hhu.bsinfo.utils.UnsafeMemory;
import de.hhu.bsinfo.utils.serialization.Importable;

/**
 * Implementation of an Importer for network messages.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.07.2017
 */
class MessageImporterDefault extends AbstractMessageImporter {

    private long m_bufferAddress;
    private int m_currentPosition;
    private int m_startPosition;

    /**
     * Constructor
     */
    MessageImporterDefault() {

    }

    @Override
    public String toString() {
        return "m_bufferAddress 0x" + Long.toHexString(m_bufferAddress) + ", m_currentPosition " + m_currentPosition + ", m_startPosition " + m_startPosition;
    }

    @Override
    public int getPosition() {
        return m_currentPosition;
    }

    @Override
    public int getNumberOfReadBytes() {
        return m_currentPosition - m_startPosition;
    }

    @Override
    public void setBuffer(final long p_addr, final int p_size, final int p_position) {
        m_bufferAddress = p_addr;
        m_currentPosition = p_position;
        m_startPosition = p_position;
    }

    @Override
    public void setUnfinishedOperation(final UnfinishedImExporterOperation p_unfinishedOperation) {
        // Not relevant for this importer
    }

    @Override
    public void setNumberOfReadBytes(int p_numberOfReadBytes) {
        // Not relevant for this importer
    }

    @Override
    public void importObject(final Importable p_object) {
        p_object.importObject(this);
    }

    @Override
    public boolean readBoolean(final boolean p_bool) {
        boolean ret = UnsafeMemory.readByte(m_bufferAddress + m_currentPosition) == 1;
        m_currentPosition++;
        return ret;
    }

    @Override
    public byte readByte(final byte p_byte) {
        byte ret = UnsafeMemory.readByte(m_bufferAddress + m_currentPosition);
        m_currentPosition++;
        return ret;
    }

    @Override
    public short readShort(final short p_short) {
        short ret = UnsafeMemory.readShort(m_bufferAddress + m_currentPosition);
        m_currentPosition += Short.BYTES;

        return ret;
    }

    @Override
    public int readInt(final int p_int) {
        int ret = UnsafeMemory.readInt(m_bufferAddress + m_currentPosition);
        m_currentPosition += Integer.BYTES;

        return ret;
    }

    @Override
    public long readLong(final long p_long) {
        long ret = UnsafeMemory.readLong(m_bufferAddress + m_currentPosition);
        m_currentPosition += Long.BYTES;

        return ret;
    }

    @Override
    public float readFloat(final float p_float) {
        float ret = UnsafeMemory.readFloat(m_bufferAddress + m_currentPosition);
        m_currentPosition += Float.BYTES;

        return ret;
    }

    @Override
    public double readDouble(final double p_double) {
        double ret = UnsafeMemory.readDouble(m_bufferAddress + m_currentPosition);
        m_currentPosition += Double.BYTES;

        return ret;
    }

    @Override
    public int readCompactNumber(final int p_int) {
        int ret = 0;

        for (int i = 0; i < Integer.BYTES; i++) {
            int tmp = UnsafeMemory.readByte(m_bufferAddress + m_currentPosition);
            m_currentPosition++;

            // Compact numbers are little-endian!
            ret |= (tmp & 0x7F) << i * 7;
            if ((tmp & 0x80) == 0) {
                // Highest bit unset -> no more bytes to come for this number
                break;
            }
        }

        return ret;
    }

    @Override
    public String readString(final String p_string) {
        return new String(readByteArray(null));
    }

    @Override
    public int readBytes(final byte[] p_array) {
        return readBytes(p_array, 0, p_array.length);
    }

    @Override
    public int readShorts(final short[] p_array) {
        return readShorts(p_array, 0, p_array.length);
    }

    @Override
    public int readInts(final int[] p_array) {
        return readInts(p_array, 0, p_array.length);
    }

    @Override
    public int readLongs(final long[] p_array) {
        return readLongs(p_array, 0, p_array.length);
    }

    @Override
    public int readBytes(final byte[] p_array, final int p_offset, final int p_length) {
        int ret = UnsafeMemory.readBytes(m_bufferAddress + m_currentPosition, p_array, p_offset, p_length);
        m_currentPosition += ret * Byte.BYTES;

        return ret;
    }

    @Override
    public int readShorts(final short[] p_array, final int p_offset, final int p_length) {
        int ret = UnsafeMemory.readShorts(m_bufferAddress + m_currentPosition, p_array, p_offset, p_length);
        m_currentPosition += ret * Short.BYTES;

        return ret;
    }

    @Override
    public int readInts(final int[] p_array, final int p_offset, final int p_length) {
        int ret = UnsafeMemory.readInts(m_bufferAddress + m_currentPosition, p_array, p_offset, p_length);
        m_currentPosition += ret * Integer.BYTES;

        return ret;
    }

    @Override
    public int readLongs(final long[] p_array, final int p_offset, final int p_length) {
        int ret = UnsafeMemory.readLongs(m_bufferAddress + m_currentPosition, p_array, p_offset, p_length);
        m_currentPosition += ret * Long.BYTES;

        return ret;
    }

    @Override
    public byte[] readByteArray(final byte[] p_array) {
        byte[] arr = new byte[readCompactNumber(0)];
        readBytes(arr);
        return arr;
    }

    @Override
    public short[] readShortArray(final short[] p_array) {
        short[] arr = new short[readCompactNumber(0)];
        readShorts(arr);
        return arr;
    }

    @Override
    public int[] readIntArray(final int[] p_array) {
        int[] arr = new int[readCompactNumber(0)];
        readInts(arr);
        return arr;
    }

    @Override
    public long[] readLongArray(final long[] p_array) {
        long[] arr = new long[readCompactNumber(0)];
        readLongs(arr);
        return arr;
    }
}
