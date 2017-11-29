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

import java.nio.charset.StandardCharsets;

import de.hhu.bsinfo.dxutils.UnsafeMemory;
import de.hhu.bsinfo.dxutils.serialization.Importable;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Implementation of an Importer for network messages, used after overflow and with insufficient buffer length (large messages).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.07.2017
 */
class MessageImporterUnderOverflow extends AbstractMessageImporter {

    private long m_bufferAddress;
    private int m_bufferSize;
    private int m_currentPosition;

    // Number of bytes read before and bytes already skipped
    private int m_skipBytes;
    private int m_skippedBytes;

    // The unfinished operation from last read (overflow) and when new overflow happens, stores the start of the block that's unfinished (and object)
    private UnfinishedImExporterOperation m_unfinishedOperation;

    // Re-use exception to avoid "new"
    private ArrayIndexOutOfBoundsException m_exception;

    /**
     * Constructor
     */
    MessageImporterUnderOverflow() {
        m_exception = new ArrayIndexOutOfBoundsException();
    }

    @Override
    public String toString() {
        return "m_bufferAddress 0x" + Long.toHexString(m_bufferAddress) + ", m_bufferSize " + m_bufferSize + ", m_currentPosition " + m_currentPosition +
                ", m_skipBytes " + m_skipBytes + ", m_skippedBytes " + m_skippedBytes + ", m_unfinishedOperation (" + m_unfinishedOperation + ')';
    }

    @Override
    public int getPosition() {
        return m_currentPosition;
    }

    @Override
    public int getNumberOfReadBytes() {
        return m_currentPosition + m_skipBytes;
    }

    @Override
    public void setBuffer(final long p_addr, final int p_size, final int p_position) {
        m_bufferAddress = p_addr;
        m_bufferSize = p_size;
        m_currentPosition = p_position;

        if (p_position != 0) {
            throw new IllegalStateException("Position != 0: " + p_position);
        }
    }

    @Override
    public void setUnfinishedOperation(final UnfinishedImExporterOperation p_unfinishedOperation) {
        m_unfinishedOperation = p_unfinishedOperation;
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
        if (m_currentPosition == m_bufferSize) {
            // Overflow
            m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition);
            throw m_exception;
        }

        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Boolean was read before, return passed value
            m_skippedBytes++;
            return p_bool;
        } else {
            boolean ret = UnsafeMemory.readByte(m_bufferAddress + m_currentPosition) == 1;
            m_currentPosition++;
            return ret;
        }
    }

    @Override
    public byte readByte(final byte p_byte) {
        if (m_currentPosition == m_bufferSize) {
            // Overflow
            m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition);
            throw m_exception;
        }

        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Byte was read before, return passed value
            m_skippedBytes++;
            return p_byte;
        } else {
            byte b = UnsafeMemory.readByte(m_bufferAddress + m_currentPosition);
            m_currentPosition++;
            return b;
        }
    }

    @Override
    public short readShort(final short p_short) {
        if (m_currentPosition == m_bufferSize) {
            // Overflow
            m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition);
            throw m_exception;
        }

        if (m_skippedBytes < m_skipBytes) {
            // Number of bytes to skip might be larger than Short.Bytes if this short was read before
            int count = 0;
            short ret = (short) m_unfinishedOperation.getPrimitive();
            for (int i = m_skipBytes - m_skippedBytes; i < Short.BYTES; i++) {
                if (m_currentPosition == m_bufferSize) {
                    // Overflow
                    m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition - count);
                    m_unfinishedOperation.setPrimitive(ret);
                    throw m_exception;
                }

                // read little endian byte order to big endian
                ret |= (UnsafeMemory.readByte(m_bufferAddress + m_currentPosition) & 0xFF) << i * 8;
                m_currentPosition++;
                count++;
            }
            m_skippedBytes += Short.BYTES - count;

            if (count == 0) {
                // Short was read before, return passed value
                return p_short;
            } else {
                return ret;
            }
        } else {
            // Read short normally as all previously read bytes have been skipped already
            short ret = 0;
            for (int i = 0; i < Short.BYTES; i++) {
                if (m_currentPosition == m_bufferSize) {
                    // Overflow
                    m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition - i);
                    m_unfinishedOperation.setPrimitive(ret);
                    throw m_exception;
                }

                // read little endian byte order to big endian
                ret |= (UnsafeMemory.readByte(m_bufferAddress + m_currentPosition) & 0xFF) << i * 8;
                m_currentPosition++;
            }
            return ret;
        }
    }

    @Override
    public int readInt(final int p_int) {
        if (m_currentPosition == m_bufferSize) {
            // Overflow
            m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition);
            throw m_exception;
        }

        if (m_skippedBytes < m_skipBytes) {
            // Number of bytes to skip might be larger than Integer.Bytes if this int was read before
            int count = 0;
            int ret = (int) m_unfinishedOperation.getPrimitive();
            for (int i = m_skipBytes - m_skippedBytes; i < Integer.BYTES; i++) {
                if (m_currentPosition == m_bufferSize) {
                    // Overflow
                    m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition - count);
                    m_unfinishedOperation.setPrimitive(ret);
                    throw m_exception;
                }

                // read little endian byte order to big endian
                ret |= (UnsafeMemory.readByte(m_bufferAddress + m_currentPosition) & 0xFF) << i * 8;
                m_currentPosition++;
                count++;
            }
            m_skippedBytes += Integer.BYTES - count;

            if (count == 0) {
                // Int was read before, return passed value
                return p_int;
            } else {
                return ret;
            }
        } else {
            // Read int normally as all previously read bytes have been skipped already
            int ret = 0;
            for (int i = 0; i < Integer.BYTES; i++) {
                if (m_currentPosition == m_bufferSize) {
                    // Overflow
                    m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition - i);
                    m_unfinishedOperation.setPrimitive(ret);
                    throw m_exception;
                }

                // read little endian byte order to big endian
                ret |= (UnsafeMemory.readByte(m_bufferAddress + m_currentPosition) & 0xFF) << i * 8;
                m_currentPosition++;
            }
            return ret;
        }
    }

    @Override
    public long readLong(final long p_long) {
        if (m_currentPosition == m_bufferSize) {
            // Overflow
            m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition);
            throw m_exception;
        }

        if (m_skippedBytes < m_skipBytes) {
            // Number of bytes to skip might be larger than Long.Bytes if this long was read before
            int count = 0;
            long ret = m_unfinishedOperation.getPrimitive();
            for (int i = m_skipBytes - m_skippedBytes; i < Long.BYTES; i++) {
                if (m_currentPosition == m_bufferSize) {
                    // Overflow
                    m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition - count);
                    m_unfinishedOperation.setPrimitive(ret);
                    throw m_exception;
                }

                // read little endian byte order to big endian
                ret |= (long) (UnsafeMemory.readByte(m_bufferAddress + m_currentPosition) & 0xFF) << i * 8;
                m_currentPosition++;
                count++;
            }
            m_skippedBytes += Long.BYTES - count;

            if (count == 0) {
                // Long was read before, return passed value
                return p_long;
            } else {
                return ret;
            }
        } else {
            // Read long normally as all previously read bytes have been skipped already
            long ret = 0;
            for (int i = 0; i < Long.BYTES; i++) {
                if (m_currentPosition == m_bufferSize) {
                    // Overflow
                    m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition - i);
                    m_unfinishedOperation.setPrimitive(ret);
                    throw m_exception;
                }

                // read little endian byte order to big endian
                ret |= (long) (UnsafeMemory.readByte(m_bufferAddress + m_currentPosition) & 0xFF) << i * 8;
                m_currentPosition++;
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
        if (m_currentPosition == m_bufferSize) {
            // Overflow
            m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition);
            throw m_exception;
        }

        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            m_skippedBytes += ObjectSizeUtil.sizeofCompactedNumber(p_int);
            // Compact number was read before, return passed value
            return p_int;
        } else if (m_skippedBytes < m_skipBytes) {
            int count = 0;
            // Compact number was partly de-serialized -> continue
            int ret = (int) m_unfinishedOperation.getPrimitive();
            for (int i = m_skipBytes - m_skippedBytes; i < Integer.BYTES; i++) {
                if (m_currentPosition == m_bufferSize) {
                    // Overflow
                    m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition - count);
                    m_unfinishedOperation.setPrimitive(ret);
                    throw m_exception;
                }

                int tmp = UnsafeMemory.readByte(m_bufferAddress + m_currentPosition);
                m_currentPosition++;
                count++;
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
                if (m_currentPosition == m_bufferSize) {
                    // Overflow
                    m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition - i);
                    m_unfinishedOperation.setPrimitive(ret);
                    throw m_exception;
                }

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
    }

    @Override
    public String readString(final String p_string) {
        return new String(readByteArray(p_string.getBytes(StandardCharsets.US_ASCII)));
    }

    @Override
    public int readBytes(byte[] p_array) {
        return readBytes(p_array, 0, p_array.length);
    }

    @Override
    public int readShorts(short[] p_array) {
        return readShorts(p_array, 0, p_array.length);
    }

    @Override
    public int readInts(int[] p_array) {
        return readInts(p_array, 0, p_array.length);
    }

    @Override
    public int readLongs(long[] p_array) {
        return readLongs(p_array, 0, p_array.length);
    }

    @Override
    public int readBytes(byte[] p_array, int p_offset, int p_length) {
        if (m_currentPosition == m_bufferSize) {
            // Overflow
            m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition);
            throw m_exception;
        }

        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Bytes were read before
            m_skippedBytes += p_length;
            return p_length;
        } else if (m_skippedBytes < m_skipBytes) {
            // Bytes were partly de-serialized -> continue
            int bytesCopied = m_skipBytes - m_skippedBytes;

            if (m_currentPosition + p_length - bytesCopied >= m_bufferSize) {
                // Overflow
                UnsafeMemory.readBytes(m_bufferAddress + m_currentPosition, p_array, p_offset + bytesCopied, m_bufferSize - m_currentPosition);
                m_unfinishedOperation.setIndex(m_skippedBytes);
                m_currentPosition = m_bufferSize;
                m_skippedBytes = m_skipBytes;
                throw m_exception;
            }

            UnsafeMemory.readBytes(m_bufferAddress + m_currentPosition, p_array, p_offset + bytesCopied, p_length - bytesCopied);
            m_currentPosition += p_length - bytesCopied;
            m_skippedBytes = m_skipBytes;

            return p_length;
        } else {
            if (m_currentPosition + p_length >= m_bufferSize) {
                // Overflow
                UnsafeMemory.readBytes(m_bufferAddress + m_currentPosition, p_array, p_offset, m_bufferSize - m_currentPosition);
                m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition);
                m_currentPosition = m_bufferSize;
                throw m_exception;
            }

            // Read bytes normally as all previously read bytes have been skipped already
            UnsafeMemory.readBytes(m_bufferAddress + m_currentPosition, p_array, p_offset, p_length);
            m_currentPosition += p_length;

            return p_length;
        }
    }

    @Override
    public int readShorts(short[] p_array, int p_offset, int p_length) {
        if (m_currentPosition == m_bufferSize) {
            // Overflow
            m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition);
            throw m_exception;
        }

        for (int i = 0; i < p_length; i++) {
            p_array[p_offset + i] = readShort(p_array[p_offset + i]);
        }

        return p_length;
    }

    @Override
    public int readInts(int[] p_array, int p_offset, int p_length) {
        if (m_currentPosition == m_bufferSize) {
            // Overflow
            m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition);
            throw m_exception;
        }

        for (int i = 0; i < p_length; i++) {
            p_array[p_offset + i] = readInt(p_array[p_offset + i]);
        }

        return p_length;
    }

    @Override
    public int readLongs(long[] p_array, int p_offset, int p_length) {
        if (m_currentPosition == m_bufferSize) {
            // Overflow
            m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition);
            throw m_exception;
        }

        for (int i = 0; i < p_length; i++) {
            p_array[p_offset + i] = readLong(p_array[p_offset + i]);
        }

        return p_length;
    }

    @Override
    public byte[] readByteArray(final byte[] p_array) {
        if (m_currentPosition == m_bufferSize) {
            // Overflow
            m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition);
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
                m_unfinishedOperation.setIndex(startPosition);
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
                m_unfinishedOperation.setIndex(startPosition);
                m_unfinishedOperation.setObject(arr);
                throw e;
            }
            return arr;
        }
    }

    @Override
    public short[] readShortArray(final short[] p_array) {
        if (m_currentPosition == m_bufferSize) {
            // Overflow
            m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition);
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
                m_unfinishedOperation.setIndex(startPosition);
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
                m_unfinishedOperation.setIndex(startPosition);
                m_unfinishedOperation.setObject(arr);
                throw e;
            }
            return arr;
        }
    }

    @Override
    public int[] readIntArray(final int[] p_array) {
        if (m_currentPosition == m_bufferSize) {
            // Overflow
            m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition);
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
                m_unfinishedOperation.setIndex(startPosition);
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
                m_unfinishedOperation.setIndex(startPosition);
                m_unfinishedOperation.setObject(arr);
                throw e;
            }
            return arr;
        }
    }

    @Override
    public long[] readLongArray(final long[] p_array) {
        if (m_currentPosition == m_bufferSize) {
            // Overflow
            m_unfinishedOperation.setIndex(m_skippedBytes + m_currentPosition);
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
                m_unfinishedOperation.setIndex(startPosition);
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
                m_unfinishedOperation.setIndex(startPosition);
                m_unfinishedOperation.setObject(arr);
                throw e;
            }
            return arr;
        }
    }
}
