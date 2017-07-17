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

import java.util.Arrays;

import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.utils.serialization.CompactNumber;
import de.hhu.bsinfo.utils.serialization.Importable;

/**
 * Implementation of an Importer/Exporter for ByteBuffers.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
class NIOMessageImporterOverflow extends AbstractMessageImporter {

    private byte[] m_buffer;
    private int m_currentPosition;
    private int m_startPosition;
    private int m_limit;
    private byte[] m_leftover;
    private byte[] m_compactNumber;

    /**
     * Constructor
     */
    NIOMessageImporterOverflow() {
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
    public byte[] getLeftover() {
        return m_leftover;
    }

    @Override
    public byte[] getCompactedNumber() {
        return m_compactNumber;
    }

    @Override
    protected void setBuffer(final byte[] p_buffer, final int p_position, final int p_limit) {
        m_buffer = p_buffer;
        m_currentPosition = p_position;
        m_startPosition = p_position;
        m_limit = p_limit;
    }

    @Override
    public void setNumberOfReadBytes(int p_numberOfReadBytes) {
        // Not relevant for this importer
    }

    @Override
    public void setLeftover(final byte[] p_leftover) {
        m_leftover = p_leftover;
    }

    @Override
    public void setCompactedNumber(byte[] p_compactedNumber) {
        m_compactNumber = p_compactedNumber;
    }

    @Override
    public void importObject(final Importable p_object) {
        p_object.importObject(this);
    }

    @Override
    public boolean readBoolean(final boolean p_bool) {
        // System.out.println("\t\tReading boolean: " + m_currentPosition + " (overflow)");
        if (m_currentPosition == m_limit) {
            // TODO: Pool exceptions?
            throw new ArrayIndexOutOfBoundsException();
        }
        return m_buffer[m_currentPosition++] == 1;
    }

    @Override
    public byte readByte(final byte p_byte) {
        // System.out.println("\t\tReading byte: " + m_currentPosition + " (overflow)");
        if (m_currentPosition == m_limit) {
            throw new ArrayIndexOutOfBoundsException();
        }
        return m_buffer[m_currentPosition++];
    }

    @Override
    public short readShort(final short p_short) {
        // System.out.println("\t\tReading short: " + m_currentPosition + " (overflow)");
        short ret = 0;

        // TODO: Do not use leftover array
        if (m_currentPosition + Short.BYTES == m_limit) {
            System.arraycopy(m_buffer, m_currentPosition, m_leftover, 0, m_limit - m_currentPosition);
            m_currentPosition = m_limit;
            throw new ArrayIndexOutOfBoundsException();
        }

        for (int i = 0; i < Short.BYTES; i++) {
            ret <<= 8;
            ret ^= (short) m_buffer[m_currentPosition++] & 0xff;
        }
        return ret;
    }

    @Override
    public int readInt(final int p_int) {
        // System.out.println("\t\tReading int: " + m_currentPosition + " (overflow)");
        int ret = 0;

        if (m_currentPosition + Integer.BYTES >= m_limit) {
            System.arraycopy(m_buffer, m_currentPosition, m_leftover, 0, m_limit - m_currentPosition);
            m_currentPosition = m_limit;
            throw new ArrayIndexOutOfBoundsException();
        }

        for (int i = 0; i < Integer.BYTES; i++) {
            ret <<= 8;
            ret ^= (int) m_buffer[m_currentPosition++] & 0xff;
        }
        return ret;
    }

    @Override
    public long readLong(final long p_long) {
        // System.out.println("\t\tReading long: " + m_currentPosition + " (overflow)");
        long ret = 0;

        if (m_currentPosition + Long.BYTES >= m_limit) {
            System.arraycopy(m_buffer, m_currentPosition, m_leftover, 0, m_limit - m_currentPosition);
            m_currentPosition = m_limit;
            throw new ArrayIndexOutOfBoundsException();
        }

        for (int i = 0; i < Long.BYTES; i++) {
            ret <<= 8;
            ret ^= (long) m_buffer[m_currentPosition++] & 0xff;
        }
        return ret;
    }

    @Override
    public float readFloat(final float p_float) {
        // System.out.println("\t\tReading float: " + m_currentPosition + " (overflow)");
        return Float.intBitsToFloat(readInt(0));
    }

    @Override
    public double readDouble(final double p_double) {
        // System.out.println("\t\tReading double: " + m_currentPosition + " (overflow)");
        return Double.longBitsToDouble(readLong(0));
    }

    @Override
    public int readCompactNumber(int p_int) {
        // System.out.println("\t\tReading compact number: " + m_currentPosition + " (overflow)");
        Arrays.fill(m_compactNumber, (byte) 0);
        int i;
        for (i = 0; i < Integer.BYTES; i++) {
            m_compactNumber[i] = readByte((byte) 0);
            // System.out.println(m_compactNumber[i]);
            if ((m_compactNumber[i] & 0x80) == 0) {
                break;
            }
        }

        return CompactNumber.decompact(m_compactNumber, 0, i);
    }

    @Override
    public String readString(final String p_string) {
        // System.out.println("\t\tReading string: " + m_currentPosition + " (overflow)");
        byte[] arr = new byte[readCompactNumber(0)];
        try {
            readBytes(arr);
        } catch (final ArrayIndexOutOfBoundsException ignore) {
            // Return incomplete string to continue later
            return new String(arr);
        }

        return new String(arr);
    }

    @Override
    public int readBytes(final byte[] p_array) {
        return readBytes(p_array, 0, p_array.length);
    }

    @Override
    public int readBytes(final byte[] p_array, final int p_offset, final int p_length) {
        // System.out.println("\t\tReading bytes (" + p_length + "): " + m_currentPosition + " (overflow)");

        if (m_currentPosition + p_length >= m_limit) {
            System.arraycopy(m_buffer, m_currentPosition, p_array, p_offset, m_limit - m_currentPosition);
            m_currentPosition = m_limit;
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
        // System.out.println("\t\tReading byte array: " + m_currentPosition + " (overflow)");
        byte[] arr = new byte[readCompactNumber(0)];
        readBytes(arr);
        return arr;
    }

    @Override
    public short[] readShortArray(final short[] p_array) {
        // System.out.println("\t\tReading short array: " + m_currentPosition + " (overflow)");
        short[] arr = new short[readCompactNumber(0)];
        readShorts(arr);
        return arr;
    }

    @Override
    public int[] readIntArray(final int[] p_array) {
        // System.out.println("\t\tReading int array: " + m_currentPosition + " (overflow)");
        int[] arr = new int[readCompactNumber(0)];
        readInts(arr);
        return arr;
    }

    @Override
    public long[] readLongArray(final long[] p_array) {
        // System.out.println("\t\tReading long array: " + m_currentPosition + " (overflow)");
        long[] arr = new long[readCompactNumber(0)];
        readLongs(arr);
        return arr;
    }

    @Override
    public String[] readStringArray(final String[] p_array) {
        // System.out.println("\t\tReading string array: " + m_currentPosition + " (overflow)");
        String[] strings = new String[readCompactNumber(0)];
        for (int i = 0; i < strings.length; i++) {
            strings[i] = readString(null);
        }

        return strings;
    }

}
