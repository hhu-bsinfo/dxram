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
import de.hhu.bsinfo.net.core.UnfinishedImporterOperation;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * Implementation of an Importer for byte arrays with insufficient length after overflow (very large messages).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.07.2017
 */
class NIOMessageImporterUnderOverflow extends AbstractMessageImporter {

    private byte[] m_buffer;
    private int m_currentPosition;
    private int m_startPosition;

    // Number of bytes read before and bytes already skipped
    private int m_skipBytes;
    private int m_skippedBytes;

    // This is the end of given buffer (might differ from buffer's length)
    private int m_limit;

    // The unfinished operation from last read (if there is one) and object to store the new unfinished operation in (if there is one)
    private UnfinishedImporterOperation m_unfinishedOperation;

    // Re-use exception to avoid "new"
    private ArrayIndexOutOfBoundsException m_exception;

    /**
     * Constructor
     */
    NIOMessageImporterUnderOverflow(final UnfinishedImporterOperation p_unfinishedOperation) {
        m_unfinishedOperation = p_unfinishedOperation;
        m_exception = new ArrayIndexOutOfBoundsException();
    }

    @Override
    protected int getPosition() {
        return m_currentPosition;
    }

    @Override
    public int getNumberOfReadBytes() {
        return m_currentPosition - m_startPosition + m_skipBytes;
    }

    @Override
    public void setNumberOfReadBytes(int p_numberOfReadBytes) {
        m_skipBytes = p_numberOfReadBytes;
        m_skippedBytes = 0;
    }

    @Override
    public void importObject(final Importable p_object) {
        p_object.importObject(this);
    }

    @Override
    public boolean readBoolean(final boolean p_bool) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Boolean was read before, return passed value
            m_skippedBytes++;
            return p_bool;
        } else {
            return m_buffer[m_currentPosition++] == 1;
        }
    }

    @Override
    public byte readByte(final byte p_byte) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Byte was read before, return passed value
            m_skippedBytes++;
            return p_byte;
        } else {
            return m_buffer[m_currentPosition++];
        }
    }

    @Override
    public short readShort(final short p_short) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            m_skippedBytes += Short.BYTES;
            // Short was read before, return passed value
            return p_short;
        } else if (m_skippedBytes < m_skipBytes) {
            // Short was partly de-serialized -> continue
            short ret = (short) m_unfinishedOperation.getPrimitive();
            for (int i = m_skipBytes - m_skippedBytes; i < Short.BYTES; i++) {
                ret |= (m_buffer[m_currentPosition++] & 0xFF) << (Short.BYTES - 1 - i) * 8;
            }
            m_skippedBytes = m_skipBytes;
            return ret;
        } else {
            // Read short normally as all previously read bytes have been skipped already
            short ret = 0;
            for (int i = 0; i < Short.BYTES; i++) {
                ret |= (m_buffer[m_currentPosition++] & 0xFF) << (Short.BYTES - 1 - i) * 8;
            }
            return ret;
        }
    }

    @Override
    public int readInt(final int p_int) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            m_skippedBytes += Integer.BYTES;
            // Int was read before, return passed value
            return p_int;
        } else if (m_skippedBytes < m_skipBytes) {
            // Int was partly de-serialized -> continue
            int ret = (int) m_unfinishedOperation.getPrimitive();
            for (int i = m_skipBytes - m_skippedBytes; i < Integer.BYTES; i++) {
                if (m_currentPosition == m_limit) {
                    // Overflow
                    m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition - i + m_skipBytes);
                    m_unfinishedOperation.setPrimitive(ret);
                    throw m_exception;
                }

                ret |= (m_buffer[m_currentPosition++] & 0xFF) << (Integer.BYTES - 1 - i) * 8;
            }
            m_skippedBytes = m_skipBytes;
            return ret;
        } else {
            // Read int normally as all previously read bytes have been skipped already
            int ret = 0;
            for (int i = 0; i < Integer.BYTES; i++) {
                if (m_currentPosition == m_limit) {
                    // Overflow
                    m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition - i + m_skipBytes);
                    m_unfinishedOperation.setPrimitive(ret);
                    throw m_exception;
                }

                ret |= (m_buffer[m_currentPosition++] & 0xFF) << (Integer.BYTES - 1 - i) * 8;
            }
            return ret;
        }
    }

    @Override
    public long readLong(final long p_long) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            m_skippedBytes += Long.BYTES;
            // Long was read before, return passed value
            return p_long;
        } else if (m_skippedBytes < m_skipBytes) {
            // Long was partly de-serialized -> continue
            long ret = m_unfinishedOperation.getPrimitive();
            for (int i = m_skipBytes - m_skippedBytes; i < Long.BYTES; i++) {
                if (m_currentPosition == m_limit) {
                    // Overflow
                    m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition - i + m_skipBytes);
                    m_unfinishedOperation.setPrimitive(ret);
                    throw m_exception;
                }

                ret |= ((long) m_buffer[m_currentPosition++] & 0xFF) << (Long.BYTES - 1 - i) * 8;
            }
            m_skippedBytes = m_skipBytes;
            return ret;
        } else {
            // Read long normally as all previously read bytes have been skipped already
            long ret = 0;
            for (int i = 0; i < Long.BYTES; i++) {
                if (m_currentPosition == m_limit) {
                    // Overflow
                    m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition - i + m_skipBytes);
                    m_unfinishedOperation.setPrimitive(ret);
                    throw m_exception;
                }

                ret |= ((long) m_buffer[m_currentPosition++] & 0xFF) << (Long.BYTES - 1 - i) * 8;
            }
            return ret;
        }
    }

    @Override
    public float readFloat(final float p_float) {
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            m_skippedBytes += Float.BYTES;
            // Float was read before, return passed value
            return p_float;
        } else {
            return Float.intBitsToFloat(readInt(0));
        }
    }

    @Override
    public double readDouble(final double p_double) {
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            m_skippedBytes += Double.BYTES;
            // Double was read before, return passed value
            return p_double;
        } else {
            return Double.longBitsToDouble(readLong(0));
        }
    }

    @Override
    public int readCompactNumber(int p_int) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            m_skippedBytes += ObjectSizeUtil.sizeofCompactedNumber(p_int);
            // Compact number was read before, return passed value
            return p_int;
        } else if (m_skippedBytes < m_skipBytes) {
            // Compact number was partly de-serialized -> continue
            int ret = (int) m_unfinishedOperation.getPrimitive();
            for (int i = m_skipBytes - m_skippedBytes; i < Integer.BYTES; i++) {
                if (m_currentPosition == m_limit) {
                    // Overflow
                    m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition - i + m_skipBytes);
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
            m_skippedBytes = m_skipBytes;
            return ret;
        } else {
            // Read compact number normally as all previously read bytes have been skipped already
            int ret = 0;
            for (int i = 0; i < Integer.BYTES; i++) {
                if (m_currentPosition == m_limit) {
                    // Overflow
                    m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition - i + m_skipBytes);
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
    }

    @Override
    public String readString(final String p_string) {
        return new String(readByteArray(p_string.getBytes()));
    }

    @Override
    public int readBytes(final byte[] p_array) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        return readBytes(p_array, 0, p_array.length);
    }

    @Override
    public int readBytes(final byte[] p_array, final int p_offset, final int p_length) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Bytes were read before
            m_skippedBytes += p_length;
            return p_length;
        } else if (m_skippedBytes < m_skipBytes) {
            // Bytes were partly de-serialized -> continue
            int bytesCopied = m_skipBytes - m_skippedBytes;

            if (m_currentPosition + p_length - bytesCopied >= m_limit) {
                // Overflow
                System.arraycopy(m_buffer, m_currentPosition, p_array, p_offset + bytesCopied, m_limit - m_currentPosition);
                m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
                m_currentPosition = m_limit;
                m_skippedBytes = m_skipBytes;
                throw m_exception;
            }

            System.arraycopy(m_buffer, m_currentPosition, p_array, p_offset + bytesCopied, p_length - bytesCopied);
            m_currentPosition += p_length - bytesCopied;
            m_skippedBytes = m_skipBytes;

            return p_length;
        } else {
            if (m_currentPosition + p_length >= m_limit) {
                // Overflow
                System.arraycopy(m_buffer, m_currentPosition, p_array, p_offset, m_limit - m_currentPosition);
                m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
                m_currentPosition = m_limit;
                throw m_exception;
            }

            // Read bytes normally as all previously read bytes have been skipped already
            System.arraycopy(m_buffer, m_currentPosition, p_array, p_offset, p_length);
            m_currentPosition += p_length;

            return p_length;
        }
    }

    @Override
    public int readShorts(final short[] p_array) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        return readShorts(p_array, 0, p_array.length);
    }

    @Override
    public int readInts(final int[] p_array) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        return readInts(p_array, 0, p_array.length);
    }

    @Override
    public int readLongs(final long[] p_array) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        return readLongs(p_array, 0, p_array.length);
    }

    @Override
    public int readShorts(final short[] p_array, final int p_offset, final int p_length) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        for (int i = 0; i < p_length; i++) {
            p_array[p_offset + i] = readShort(p_array[p_offset + i]);
        }

        return p_length;
    }

    @Override
    public int readInts(final int[] p_array, final int p_offset, final int p_length) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        for (int i = 0; i < p_length; i++) {
            p_array[p_offset + i] = readInt(p_array[p_offset + i]);
        }

        return p_length;
    }

    @Override
    public int readLongs(final long[] p_array, final int p_offset, final int p_length) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        for (int i = 0; i < p_length; i++) {
            p_array[p_offset + i] = readLong(p_array[p_offset + i]);
        }

        return p_length;
    }

    @Override
    public byte[] readByteArray(final byte[] p_array) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        int startPosition = m_currentPosition;
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Array length and array were read before, return passed array
            m_skippedBytes += ObjectSizeUtil.sizeofCompactedNumber(p_array.length) + p_array.length;
            return p_array;
        } else if (m_skippedBytes < m_skipBytes) {
            // Byte array was partly de-serialized -> continue
            byte[] arr;
            if (m_unfinishedOperation.getObject() == null) {
                // Array length has not been read completely
                arr = new byte[readCompactNumber(0)];
            } else {
                // Array was created before but is incomplete
                arr = (byte[]) m_unfinishedOperation.getObject();
                m_skippedBytes += ObjectSizeUtil.sizeofCompactedNumber(arr.length);
            }
            try {
                readBytes(arr);
            } catch (final ArrayIndexOutOfBoundsException e) {
                // Store partly de-serialized array to be finished later
                m_unfinishedOperation.setIndex(startPosition - m_startPosition + m_skipBytes);
                m_unfinishedOperation.setObject(arr);
                throw e;
            }
            return arr;
        } else {
            // Read bytes normally as all previously read bytes have been skipped already
            byte[] arr = new byte[readCompactNumber(0)];
            try {
                readBytes(arr);
            } catch (final ArrayIndexOutOfBoundsException e) {
                // Store partly de-serialized array to be finished later
                m_unfinishedOperation.setIndex(startPosition - m_startPosition + m_skipBytes);
                m_unfinishedOperation.setObject(arr);
                throw e;
            }
            return arr;
        }
    }

    @Override
    public short[] readShortArray(final short[] p_array) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        int startPosition = m_currentPosition;
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Array length and array were read before, return passed array
            m_skippedBytes += ObjectSizeUtil.sizeofCompactedNumber(p_array.length) + p_array.length * Short.BYTES;
            return p_array;
        } else if (m_skippedBytes < m_skipBytes) {
            // Short array was partly de-serialized -> continue
            short[] arr;
            if (m_unfinishedOperation.getObject() == null) {
                // Array length has not been read completely
                arr = new short[readCompactNumber(0)];
            } else {
                // Array was created before but is incomplete
                arr = (short[]) m_unfinishedOperation.getObject();
                m_skippedBytes += ObjectSizeUtil.sizeofCompactedNumber(arr.length);
            }
            try {
                readShorts(arr);
            } catch (final ArrayIndexOutOfBoundsException e) {
                // Store partly de-serialized array to be finished later
                m_unfinishedOperation.setIndex(startPosition - m_startPosition + m_skipBytes);
                m_unfinishedOperation.setObject(arr);
                throw e;
            }
            return arr;
        } else {
            // Read shorts normally as all previously read bytes have been skipped already
            short[] arr = new short[readCompactNumber(0)];
            try {
                readShorts(arr);
            } catch (final ArrayIndexOutOfBoundsException e) {
                // Store partly de-serialized array to be finished later
                m_unfinishedOperation.setIndex(startPosition - m_startPosition + m_skipBytes);
                m_unfinishedOperation.setObject(arr);
                throw e;
            }
            return arr;
        }
    }

    @Override
    public int[] readIntArray(final int[] p_array) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        int startPosition = m_currentPosition;
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Array length and array were read before, return passed array
            m_skippedBytes += ObjectSizeUtil.sizeofCompactedNumber(p_array.length) + p_array.length * Integer.BYTES;
            return p_array;
        } else if (m_skippedBytes < m_skipBytes) {
            // Int array was partly de-serialized -> continue
            int[] arr;
            if (m_unfinishedOperation.getObject() == null) {
                // Array length has not been read completely
                arr = new int[readCompactNumber(0)];
            } else {
                // Array was created before but is incomplete
                arr = (int[]) m_unfinishedOperation.getObject();
                m_skippedBytes += ObjectSizeUtil.sizeofCompactedNumber(arr.length);
            }
            try {
                readInts(arr);
            } catch (final ArrayIndexOutOfBoundsException e) {
                // Store partly de-serialized array to be finished later
                m_unfinishedOperation.setIndex(startPosition - m_startPosition + m_skipBytes);
                m_unfinishedOperation.setObject(arr);
                throw e;
            }
            return arr;
        } else {
            // Read integers normally as all previously read bytes have been skipped already
            int[] arr = new int[readCompactNumber(0)];
            try {
                readInts(arr);
            } catch (final ArrayIndexOutOfBoundsException e) {
                // Store partly de-serialized array to be finished later
                m_unfinishedOperation.setIndex(startPosition - m_startPosition + m_skipBytes);
                m_unfinishedOperation.setObject(arr);
                throw e;
            }
            return arr;
        }
    }

    @Override
    public long[] readLongArray(final long[] p_array) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        int startPosition = m_currentPosition;
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Array length and array were read before, return passed array
            m_skippedBytes += ObjectSizeUtil.sizeofCompactedNumber(p_array.length) + p_array.length * Long.BYTES;
            return p_array;
        } else if (m_skippedBytes < m_skipBytes) {
            // Long array was partly de-serialized -> continue
            long[] arr;
            if (m_unfinishedOperation.getObject() == null) {
                // Array length has not been read completely
                arr = new long[readCompactNumber(0)];
            } else {
                // Array was created before but is incomplete
                arr = (long[]) m_unfinishedOperation.getObject();
                m_skippedBytes += ObjectSizeUtil.sizeofCompactedNumber(arr.length);
            }
            try {
                readLongs(arr);
            } catch (final ArrayIndexOutOfBoundsException e) {
                // Store partly de-serialized array to be finished later
                m_unfinishedOperation.setIndex(startPosition - m_startPosition + m_skipBytes);
                m_unfinishedOperation.setObject(arr);
                throw e;
            }
            return arr;
        } else {
            // Read longs normally as all previously read bytes have been skipped already
            long[] arr = new long[readCompactNumber(0)];
            try {
                readLongs(arr);
            } catch (final ArrayIndexOutOfBoundsException e) {
                // Store partly de-serialized array to be finished later
                m_unfinishedOperation.setIndex(startPosition - m_startPosition + m_skipBytes);
                m_unfinishedOperation.setObject(arr);
                throw e;
            }
            return arr;
        }
    }

    @Override
    public String[] readStringArray(final String[] p_array) {
        if (m_currentPosition == m_limit) {
            // Overflow
            m_unfinishedOperation.setIndex(m_currentPosition - m_startPosition + m_skipBytes);
            throw m_exception;
        }

        int startPosition = m_currentPosition;
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Array length and array were read before, return passed array
            m_skippedBytes += ObjectSizeUtil.sizeofStringArray(p_array);
            return p_array;
        } else if (m_skippedBytes < m_skipBytes) {
            // String array was partly de-serialized -> continue
            String[] arr;
            if (m_unfinishedOperation.getObject() == null) {
                // Array length has not been read completely
                arr = new String[readCompactNumber(0)];
            } else {
                // Array was created before but is incomplete
                arr = (String[]) m_unfinishedOperation.getObject();
                m_skippedBytes += ObjectSizeUtil.sizeofCompactedNumber(arr.length);
            }
            try {
                for (int i = 0; i < arr.length; i++) {
                    arr[i] = readString(arr[i]);
                }
            } catch (final ArrayIndexOutOfBoundsException e) {
                // Store partly de-serialized array to be finished later
                m_unfinishedOperation.setIndex(startPosition - m_startPosition + m_skipBytes);
                m_unfinishedOperation.setObject(arr);
                throw e;
            }
            return arr;
        } else {
            // Read Strings normally as all previously read bytes have been skipped already
            String[] arr = new String[readCompactNumber(0)];
            try {
                for (int i = 0; i < arr.length; i++) {
                    arr[i] = readString(arr[i]);
                }
            } catch (final ArrayIndexOutOfBoundsException e) {
                // Store partly de-serialized array to be finished later
                m_unfinishedOperation.setIndex(startPosition - m_startPosition + m_skipBytes);
                m_unfinishedOperation.setObject(arr);
                throw e;
            }
            return arr;
        }
    }

    void setBuffer(final byte[] p_buffer, final int p_position, final int p_limit) {
        m_buffer = p_buffer;
        m_currentPosition = p_position;
        m_startPosition = p_position;
        m_limit = p_limit;
    }
}
