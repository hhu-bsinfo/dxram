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

package de.hhu.bsinfo.net.nio;

import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.utils.serialization.Importable;

/**
 * Implementation of an Importer for byte arrays.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.07.2017
 */
class NIOMessageImporter extends AbstractMessageImporter {

    private byte[] m_buffer;
    private int m_currentPosition;
    private int m_startPosition;

    /**
     * Constructor
     */
    NIOMessageImporter() {
    }

    @Override
    protected int getPosition() {
        return m_currentPosition;
    }

    @Override
    public int getNumberOfReadBytes() {
        return m_currentPosition - m_startPosition;
    }

    @Override
    public void setNumberOfReadBytes(int p_numberOfReadBytes) {
        // Not relevant for this importer
    }

    @Override
    protected void setBuffer(final byte[] p_buffer, final int p_position, final int p_limit) {
        m_buffer = p_buffer;
        m_currentPosition = p_position;
        m_startPosition = p_position;
    }

    @Override
    public void importObject(final Importable p_object) {
        p_object.importObject(this);
    }

    @Override
    public boolean readBoolean(final boolean p_bool) {
        return m_buffer[m_currentPosition++] == 1;
    }

    @Override
    public byte readByte(final byte p_byte) {
        return m_buffer[m_currentPosition++];
    }

    @Override
    public short readShort(final short p_short) {
        short ret = 0;
        for (int i = 0; i < Short.BYTES; i++) {
            ret |= (m_buffer[m_currentPosition++] & 0xFF) << (Short.BYTES - 1 - i) * 8;
        }

        return ret;
    }

    @Override
    public int readInt(final int p_int) {
        int ret = 0;
        for (int i = 0; i < Integer.BYTES; i++) {
            ret |= (m_buffer[m_currentPosition++] & 0xFF) << (Integer.BYTES - 1 - i) * 8;
        }
        return ret;
    }

    @Override
    public long readLong(final long p_long) {
        long ret = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            ret |= ((long) m_buffer[m_currentPosition++] & 0xFF) << (Long.BYTES - 1 - i) * 8;
        }
        return ret;
    }

    @Override
    public float readFloat(final float p_float) {
        return Float.intBitsToFloat(readInt(0));
    }

    @Override
    public double readDouble(final double p_double) {
        return Double.longBitsToDouble(readLong(0));
    }

    @Override
    public int readCompactNumber(int p_int) {
        int ret = 0;

        for (int i = 0; i < Integer.BYTES; i++) {
            int tmp = m_buffer[m_currentPosition++];
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
    public int readBytes(final byte[] p_array, final int p_offset, final int p_length) {
        System.arraycopy(m_buffer, m_currentPosition, p_array, p_offset, p_length);
        m_currentPosition += p_length;

        return p_length;
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
    public int readShorts(final short[] p_array, final int p_offset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            p_array[p_offset + i] = readShort((short) 0);
        }

        return p_length;
    }

    @Override
    public int readInts(final int[] p_array, final int p_offset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            p_array[p_offset + i] = readInt(0);
        }

        return p_length;
    }

    @Override
    public int readLongs(final long[] p_array, final int p_offset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            p_array[p_offset + i] = readLong(0);
        }

        return p_length;
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

    @Override
    public String[] readStringArray(final String[] p_array) {
        String[] strings = new String[readCompactNumber(0)];
        for (int i = 0; i < strings.length; i++) {
            strings[i] = readString(null);
        }

        return strings;
    }

}
