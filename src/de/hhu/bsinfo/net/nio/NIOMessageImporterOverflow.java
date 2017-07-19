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
import de.hhu.bsinfo.net.core.AbstractMessageImporterCollection;
import de.hhu.bsinfo.utils.serialization.Importable;

/**
 * Implementation of an Importer for byte arrays with insufficient length.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.07.2017
 */
class NIOMessageImporterOverflow extends AbstractMessageImporter {

    private byte[] m_buffer;
    private int m_currentPosition;
    private int m_startPosition;

    // This is the end of given buffer (might differ from buffer's length)
    private int m_limit;

    // Object to store the unfinished operation in (if there is one)
    private AbstractMessageImporterCollection.UnfinishedOperation m_unfinishedOperation;

    // Re-use exception to avoid "new"
    private ArrayIndexOutOfBoundsException m_exception;

    /**
     * Constructor
     */
    NIOMessageImporterOverflow(final AbstractMessageImporterCollection.UnfinishedOperation p_unfinishedOperation) {
        m_unfinishedOperation = p_unfinishedOperation;
        m_exception = new ArrayIndexOutOfBoundsException();
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
        m_limit = p_limit;
    }

    @Override
    public void importObject(final Importable p_object) {
        p_object.importObject(this);
    }

    @Override
    public boolean readBoolean(final boolean p_bool) {
        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        return m_buffer[m_currentPosition++] == 1;
    }

    @Override
    public byte readByte(final byte p_byte) {
        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        return m_buffer[m_currentPosition++];
    }

    @Override
    public short readShort(final short p_short) {
        short ret = 0;

        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        for (int i = 0; i < Short.BYTES; i++) {
            if (m_currentPosition == m_limit) {
                // Store unfinished short and throw exception to continue later
                m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition - i);
                m_unfinishedOperation.setPrimitive(ret);
                throw m_exception;
            }

            ret |= (m_buffer[m_currentPosition++] & 0xFF) << (Short.BYTES - 1 - i) * 8;
        }

        return ret;
    }

    @Override
    public int readInt(final int p_int) {
        int ret = 0;

        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        for (int i = 0; i < Integer.BYTES; i++) {
            if (m_currentPosition == m_limit) {
                // Store unfinished int and throw exception to continue later
                m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition - i);
                m_unfinishedOperation.setPrimitive(ret);
                throw m_exception;
            }

            ret |= (m_buffer[m_currentPosition++] & 0xFF) << (Integer.BYTES - 1 - i) * 8;
        }

        return ret;
    }

    @Override
    public long readLong(final long p_long) {
        long ret = 0;

        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        for (int i = 0; i < Long.BYTES; i++) {
            if (m_currentPosition == m_limit) {
                // Store unfinished long and throw exception to continue later
                m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition - i);
                m_unfinishedOperation.setPrimitive(ret);
                throw m_exception;
            }

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

        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        for (int i = 0; i < Integer.BYTES; i++) {
            if (m_currentPosition == m_limit) {
                // Store unfinished compact number and start index of unfinished compact number (needed in underflow importer)
                // Throw exception to continue later
                m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition - i);
                m_unfinishedOperation.setPrimitive(ret);
                throw m_exception;
            }

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

        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        return readBytes(p_array, 0, p_array.length);
    }

    @Override
    public int readBytes(final byte[] p_array, final int p_offset, final int p_length) {
        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        if (m_currentPosition + p_length >= m_limit) {
            System.arraycopy(m_buffer, m_currentPosition, p_array, p_offset, m_limit - m_currentPosition);
            // Do not store unfinished operation as partly de-serialized array will be passed anyway
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            m_currentPosition = m_limit;
            throw m_exception;
        }

        System.arraycopy(m_buffer, m_currentPosition, p_array, p_offset, p_length);
        m_currentPosition += p_length;

        return p_length;
    }

    @Override
    public int readShorts(final short[] p_array) {

        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }
        // Exception might be thrown in readShorts
        // Do not store unfinished operation as partly de-serialized array will be passed anyway
        return readShorts(p_array, 0, p_array.length);
    }

    @Override
    public int readInts(final int[] p_array) {

        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        // Exception might be thrown in readInts
        // Do not store unfinished operation as partly de-serialized array will be passed anyway
        return readInts(p_array, 0, p_array.length);
    }

    @Override
    public int readLongs(final long[] p_array) {

        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        // Exception might be thrown in readLongs
        // Do not store unfinished operation as partly de-serialized array will be passed anyway
        return readLongs(p_array, 0, p_array.length);
    }

    @Override
    public int readShorts(final short[] p_array, final int p_offset, final int p_length) {

        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        for (int i = 0; i < p_length; i++) {
            // Exception might be thrown in readShort
            // Do not store unfinished operation as partly de-serialized array will be passed anyway
            p_array[p_offset + i] = readShort((short) 0);
        }

        return p_length;
    }

    @Override
    public int readInts(final int[] p_array, final int p_offset, final int p_length) {

        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        for (int i = 0; i < p_length; i++) {
            // Exception might be thrown in readInt
            // Do not store unfinished operation as partly de-serialized array will be passed anyway
            p_array[p_offset + i] = readInt(0);
        }

        return p_length;
    }

    @Override
    public int readLongs(final long[] p_array, final int p_offset, final int p_length) {

        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        for (int i = 0; i < p_length; i++) {
            // Exception might be thrown in readLong
            // Do not store unfinished operation as partly de-serialized array will be passed anyway
            p_array[p_offset + i] = readLong(0);
        }

        return p_length;
    }

    @Override
    public byte[] readByteArray(final byte[] p_array) {
        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        int startPosition = m_currentPosition;
        byte[] arr = new byte[readCompactNumber(0)];
        try {
            readBytes(arr);
        } catch (final ArrayIndexOutOfBoundsException e) {
            // Store partly de-serialized array to be finished later
            m_unfinishedOperation.setIndex(startPosition - m_startPosition);
            m_unfinishedOperation.setObject(arr);
            throw e;
        }
        return arr;
    }

    @Override
    public short[] readShortArray(final short[] p_array) {
        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        int startPosition = m_currentPosition;
        short[] arr = new short[readCompactNumber(0)];
        try {
            readShorts(arr);
        } catch (final ArrayIndexOutOfBoundsException e) {
            // Store partly de-serialized array to be finished later
            m_unfinishedOperation.setIndex(startPosition - m_startPosition);
            m_unfinishedOperation.setObject(arr);
            throw e;
        }

        return arr;
    }

    @Override
    public int[] readIntArray(final int[] p_array) {
        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        int startPosition = m_currentPosition;
        int[] arr = new int[readCompactNumber(0)];
        try {
            readInts(arr);
        } catch (final ArrayIndexOutOfBoundsException e) {
            // Store partly de-serialized array to be finished later
            m_unfinishedOperation.setIndex(startPosition - m_startPosition);
            m_unfinishedOperation.setObject(arr);
            throw e;
        }

        return arr;
    }

    @Override
    public long[] readLongArray(final long[] p_array) {
        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        int startPosition = m_currentPosition;
        long[] arr = new long[readCompactNumber(0)];
        try {
            readLongs(arr);
        } catch (final ArrayIndexOutOfBoundsException e) {
            // Store partly de-serialized array to be finished later
            m_unfinishedOperation.setIndex(startPosition - m_startPosition);
            m_unfinishedOperation.setObject(arr);
            throw e;
        }

        return arr;
    }

    @Override
    public String[] readStringArray(final String[] p_array) {
        if (m_currentPosition == m_limit) {
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition);
            throw m_exception;
        }

        int startPosition = m_currentPosition;
        String[] strings = new String[readCompactNumber(0)];
        try {
            for (int i = 0; i < strings.length; i++) {
                strings[i] = readString(null);
            }
        } catch (final ArrayIndexOutOfBoundsException e) {
            // Store partly de-serialized array to be finished later
            m_unfinishedOperation.setIndex(startPosition - m_startPosition);
            m_unfinishedOperation.setObject(strings);
            throw e;
        }

        return strings;
    }

}
