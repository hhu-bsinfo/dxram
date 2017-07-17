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
class NIOMessageImporterUnderflow extends AbstractMessageImporter {

    private byte[] m_buffer;
    private int m_currentPosition;
    private byte[] m_leftover;
    private int m_skipBytes;
    private int m_skippedBytes;
    private byte[] m_compactNumber;

    /**
     * Constructor
     */
    NIOMessageImporterUnderflow() {
    }

    @Override
    protected int getPosition() {
        return m_currentPosition;
    }

    @Override
    public int getNumberOfReadBytes() {
        return m_currentPosition + m_skipBytes;
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
    }

    @Override
    public void setNumberOfReadBytes(int p_numberOfReadBytes) {
        m_skipBytes = p_numberOfReadBytes;
        m_skippedBytes = 0;
    }

    @Override
    public void setLeftover(byte[] p_leftover) {
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
        if (m_skippedBytes++ < m_skipBytes) {
            // System.out.println("\t\tSkipping reading boolean: " + m_currentPosition + " (underflow)");
            return p_bool;
        } else {
            // System.out.println("\t\tReading boolean: " + m_currentPosition + " (underflow)");
            return m_buffer[m_currentPosition++] == 1;
        }
    }

    @Override
    public byte readByte(final byte p_byte) {
        if (m_skippedBytes++ < m_skipBytes) {
            // System.out.println("\t\tSkipping reading byte: " + m_currentPosition + " (underflow)");
            return p_byte;
        } else {
            // System.out.println("\t\tReading byte: " + m_currentPosition + " (underflow)");
            return m_buffer[m_currentPosition++];
        }
    }

    @Override
    public short readShort(final short p_short) {
        if (m_skippedBytes + Short.BYTES < m_skipBytes) {
            m_skippedBytes += Short.BYTES;
            // System.out.println("\t\tSkipping reading short: " + m_currentPosition + " (underflow)");
            return p_short;
        } else if (m_skippedBytes < m_skipBytes) {
            // System.out.println("\t\tContinue reading short: " + m_currentPosition + " (underflow)");
            short ret = m_leftover[0];
            ret <<= 8;
            ret ^= (short) m_buffer[m_currentPosition++] & 0xff;
            m_skippedBytes++;
            return ret;
        } else {
            // TODO: Overflow might happen here as well

            // System.out.println("\t\tReading short: " + m_currentPosition + " (underflow)");
            short ret = 0;
            for (int i = 0; i < Short.BYTES; i++) {
                ret <<= 8;
                ret ^= (short) m_buffer[m_currentPosition++] & 0xff;
            }
            return ret;
        }
    }

    @Override
    public int readInt(final int p_int) {
        if (m_skippedBytes + Integer.BYTES < m_skipBytes) {
            m_skippedBytes += Integer.BYTES;
            // System.out.println("\t\tSkipping reading int: " + m_currentPosition + " (underflow)");
            return p_int;
        } else if (m_skippedBytes < m_skipBytes) {
            // System.out.println("\t\tContinue reading int: " + m_currentPosition + " (underflow)");
            int ret = 0;
            int i;
            for (i = 0; i < m_skipBytes - m_skippedBytes; i++) {
                ret <<= 8;
                ret ^= (int) m_leftover[i] & 0xff;
            }
            for (int j = i; j < Integer.BYTES; j++) {
                ret <<= 8;
                ret ^= (int) m_buffer[m_currentPosition++] & 0xff;
            }
            m_skippedBytes = m_skipBytes;
            return ret;
        } else {
            // System.out.println("\t\tReading int: " + m_currentPosition + " (underflow)");
            int ret = 0;
            for (int i = 0; i < Integer.BYTES; i++) {
                ret <<= 8;
                ret ^= (int) m_buffer[m_currentPosition++] & 0xff;
            }
            return ret;
        }
    }

    @Override
    public long readLong(final long p_long) {
        if (m_skippedBytes + Long.BYTES < m_skipBytes) {
            m_skippedBytes += Long.BYTES;
            // System.out.println("\t\tSkipping reading long: " + m_currentPosition + " (underflow)");
            return p_long;
        } else if (m_skippedBytes < m_skipBytes) {
            // System.out.println("\t\tContinue reading long: " + m_currentPosition + " (underflow)");
            int ret = 0;
            int i;
            for (i = 0; i < m_skipBytes - m_skippedBytes; i++) {
                ret <<= 8;
                ret ^= (int) m_leftover[i] & 0xff;
            }
            for (int j = i; j < Long.BYTES; j++) {
                ret <<= 8;
                ret ^= (int) m_buffer[m_currentPosition++] & 0xff;
            }
            m_skippedBytes = m_skipBytes;
            return ret;
        } else {
            // System.out.println("\t\tReading long: " + m_currentPosition + " (underflow)");
            long ret = 0;
            for (int i = 0; i < Long.BYTES; i++) {
                ret <<= 8;
                ret ^= (long) m_buffer[m_currentPosition++] & 0xff;
            }
            return ret;
        }
    }

    @Override
    public float readFloat(final float p_float) {
        return Float.intBitsToFloat(readInt(Float.floatToIntBits(p_float)));
    }

    @Override
    public double readDouble(final double p_double) {
        return Double.longBitsToDouble(readLong(Double.doubleToLongBits(p_double)));
    }

    @Override
    public int readCompactNumber(int p_int) {
        int ret;

        // TODO: Handle overflow in importerUnderflow as well!

        // System.out.println("\t\tReading compact number: " + m_currentPosition + " (underflow)");

        int i;
        for (i = 0; i < Integer.BYTES; i++) {
            m_compactNumber[i] = readByte(m_compactNumber[i]);
            if ((m_compactNumber[i] & 0x80) == 0) {
                break;
            }
        }

        ret = CompactNumber.decompact(m_compactNumber, 0, i);
        Arrays.fill(m_compactNumber, (byte) 0);

        return ret;

    }

    @Override
    public String readString(final String p_string) {
        // System.out.println("\t\tReading string: " + m_currentPosition + " (underflow)");

        return new String(readByteArray(p_string.getBytes()));
    }

    @Override
    public int readBytes(final byte[] p_array) {
        return readBytes(p_array, 0, p_array.length);
    }

    @Override
    public int readBytes(final byte[] p_array, final int p_offset, final int p_length) {

        if (m_skippedBytes + p_length < m_skipBytes) {
            m_skippedBytes += p_length;
            // System.out.println("\t\tSkipping reading bytes: " + m_currentPosition + ", " + p_length + " (underflow)");
            return p_length;
        } else if (m_skippedBytes < m_skipBytes) {
            // System.out.println("\t\tContinue reading bytes: " + m_currentPosition + " (underflow)");
            int bytesCopied = m_skipBytes - m_skippedBytes;
            System.arraycopy(m_buffer, m_currentPosition, p_array, p_offset + bytesCopied, p_length - bytesCopied);
            m_currentPosition += p_length - bytesCopied;

            return p_length;
        } else {
            // System.out.println("\t\tReading bytes: " + m_currentPosition + " (underflow)");
            System.arraycopy(m_buffer, m_currentPosition, p_array, p_offset, p_length);
            m_currentPosition += p_length;

            return p_length;
        }
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
            p_array[p_offset + i] = readShort(p_array[p_offset + i]);
        }

        return p_length;
    }

    @Override
    public int readInts(final int[] p_array, final int p_offset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            p_array[p_offset + i] = readInt(p_array[p_offset + i]);
        }

        return p_length;
    }

    @Override
    public int readLongs(final long[] p_array, final int p_offset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            p_array[p_offset + i] = readLong(p_array[p_offset + i]);
        }

        return p_length;
    }

    @Override
    public byte[] readByteArray(final byte[] p_array) {
        if (p_array == null) {
            // System.out.println("\t\tReading byte array: " + m_currentPosition + " (underflow)");
            byte[] arr = new byte[readCompactNumber(0)];
            readBytes(arr);
            return arr;
        } else if (m_skippedBytes + 4 + p_array.length < m_skipBytes) {
            // System.out.println("\t\tSkipping reading byte array: " + m_currentPosition + " (underflow)");
            m_skippedBytes += p_array.length;
            return p_array;
        } else {
            // System.out.println("\t\tContinue reading byte array: " + m_currentPosition + " (underflow)");
            readBytes(p_array);
            return p_array;
        }
    }

    @Override
    public short[] readShortArray(final short[] p_array) {
        if (p_array == null) {
            // System.out.println("\t\tReading short array: " + m_currentPosition + " (underflow)");
            short[] arr = new short[readCompactNumber(0)];
            readShorts(arr);
            return arr;
        } else if (m_skippedBytes + 4 + p_array.length * 2 < m_skipBytes) {
            // System.out.println("\t\tSkipping reading short array: " + m_currentPosition + " (underflow)");
            m_skippedBytes += p_array.length;
            return p_array;
        } else {
            // System.out.println("\t\tContinue reading short array: " + m_currentPosition + " (underflow)");
            readShorts(p_array);
            return p_array;
        }
    }

    @Override
    public int[] readIntArray(final int[] p_array) {
        if (p_array == null) {
            // System.out.println("\t\tReading int array: " + m_currentPosition + " (underflow)");
            int[] arr = new int[readCompactNumber(0)];
            readInts(arr);
            return arr;
        } else if (m_skippedBytes + 4 + p_array.length * 4 < m_skipBytes) {
            // System.out.println("\t\tSkipping reading int array: " + m_currentPosition + " (underflow)");
            m_skippedBytes += p_array.length;
            return p_array;
        } else {
            // System.out.println("\t\tContinue reading int array: " + m_currentPosition + " (underflow)");
            readInts(p_array);
            return p_array;
        }
    }

    @Override
    public long[] readLongArray(final long[] p_array) {
        if (p_array == null) {
            // System.out.println("\t\tReading long array: " + m_currentPosition + " (underflow)");
            long[] arr = new long[readCompactNumber(0)];
            readLongs(arr);
            return arr;
        } else if (m_skippedBytes + 4 + p_array.length * 8 < m_skipBytes) {
            // System.out.println("\t\tSkipping reading long array: " + m_currentPosition + " (underflow)");
            m_skippedBytes += p_array.length;
            return p_array;
        } else {
            // System.out.println("\t\tContinue reading long array: " + m_currentPosition + " (underflow)");
            readLongs(p_array);
            return p_array;
        }
    }

    @Override
    public String[] readStringArray(final String[] p_array) {
        // System.out.println("\t\tReading string array: " + m_currentPosition + " (underflow)");
        if (p_array == null) {
            String[] arr = new String[readCompactNumber(0)];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = readString(arr[i]);
            }
            return arr;
        } else {
            for (int i = 0; i < p_array.length; i++) {
                p_array[i] = readString(p_array[i]);
            }
            return p_array;
        }
    }

}
