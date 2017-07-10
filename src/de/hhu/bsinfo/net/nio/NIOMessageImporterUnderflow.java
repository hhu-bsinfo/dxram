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
 * Implementation of an Importer/Exporter for ByteBuffers.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class NIOMessageImporterUnderflow extends AbstractMessageImporter {

    private byte[] m_buffer;
    private int m_currentPosition;
    private byte[] m_leftover;
    private int m_skipBytes;
    private int m_skippedBytes;

    /**
     * Constructor
     */
    public NIOMessageImporterUnderflow() {
        m_leftover = new byte[7];
    }

    @Override
    protected int getPosition() {
        return m_currentPosition;
    }

    @Override
    protected void setBuffer(final byte[] p_buffer) {
        m_buffer = p_buffer;
    }

    @Override
    protected void setPosition(final int p_position) {
        m_currentPosition = p_position;
    }

    @Override
    public void importObject(final Importable p_object) {
        p_object.importObject(this);
        m_currentPosition += p_object.sizeofObject();
    }

    @Override
    public boolean readBoolean() {
        return m_buffer[m_currentPosition++] == 1;
    }

    @Override
    public byte readByte() {
        return m_buffer[m_currentPosition++];
    }

    @Override
    public short readShort() {
        short ret = 0;

        if (m_currentPosition + 2 >= m_buffer.length) {
            System.arraycopy(m_buffer, m_currentPosition, m_leftover, 0, m_buffer.length - m_currentPosition);
            throw new ArrayIndexOutOfBoundsException();
        }

        for (int i = 0; i < 2; i++) {
            ret <<= 8;
            ret ^= (short) m_buffer[m_currentPosition++] & 0xff;
        }
        return ret;
    }

    @Override
    public int readInt() {
        int ret = 0;

        if (m_currentPosition + 4 >= m_buffer.length) {
            System.arraycopy(m_buffer, m_currentPosition, m_leftover, 0, m_buffer.length - m_currentPosition);
            throw new ArrayIndexOutOfBoundsException();
        }

        for (int i = 0; i < 4; i++) {
            ret <<= 8;
            ret ^= (int) m_buffer[m_currentPosition++] & 0xff;
        }
        return ret;
    }

    @Override
    public long readLong() {
        long ret = 0;

        if (m_currentPosition + 8 >= m_buffer.length) {
            System.arraycopy(m_buffer, m_currentPosition, m_leftover, 0, m_buffer.length - m_currentPosition);
            throw new ArrayIndexOutOfBoundsException();
        }

        for (int i = 0; i < 8; i++) {
            ret <<= 8;
            ret ^= (long) m_buffer[m_currentPosition++] & 0xff;
        }
        return ret;
    }

    @Override
    public float readFloat() {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public double readDouble() {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public String readString() {
        return new String(readByteArray());
    }

    @Override
    public int readBytes(final byte[] p_array) {
        return readBytes(p_array, 0, p_array.length);
    }

    @Override
    public int readBytes(final byte[] p_array, final int p_offset, final int p_length) {

        if (m_currentPosition + p_length >= m_buffer.length) {
            System.arraycopy(m_buffer, m_currentPosition, p_array, p_offset, m_buffer.length - m_currentPosition);
            m_currentPosition = m_buffer.length;
            throw new ArrayIndexOutOfBoundsException();
        }

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
            p_array[p_offset + i] = readShort();
        }

        return p_length;
    }

    @Override
    public int readInts(final int[] p_array, final int p_offset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            p_array[p_offset + i] = readInt();
        }

        return p_length;
    }

    @Override
    public int readLongs(final long[] p_array, final int p_offset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            p_array[p_offset + i] = readLong();
        }

        return p_length;
    }

    @Override
    public byte[] readByteArray() {
        byte[] arr = new byte[readInt()];
        readBytes(arr);
        return arr;
    }

    @Override
    public short[] readShortArray() {
        short[] arr = new short[readInt()];
        readShorts(arr);
        return arr;
    }

    @Override
    public int[] readIntArray() {
        int[] arr = new int[readInt()];
        readInts(arr);
        return arr;
    }

    @Override
    public long[] readLongArray() {
        long[] arr = new long[readInt()];
        readLongs(arr);
        return arr;
    }

    @Override
    public String[] readStringArray() {
        String[] strings = new String[readInt()];

        for (int i = 0; i < strings.length; i++) {
            strings[i] = readString();
        }

        return strings;
    }

}
