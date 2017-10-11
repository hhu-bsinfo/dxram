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
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * Implementation of an Exporter for network messages, with insufficient space (wrap around).
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.07.2017
 */
class LargeMessageExporter extends AbstractMessageExporter {

    private long m_bufferAddress;
    private int m_bufferSize;
    private int m_currentPosition;
    private int m_startPosition;
    private int m_endPosition;

    // Number of bytes written before and bytes already skipped
    private int m_skipBytes;
    private int m_skippedBytes;

    // The unfinished operation from last read (if there is one) and object to store the new unfinished operation in (if there is one)
    private UnfinishedImExporterOperation m_unfinishedOperation;

    // Re-use exception to avoid "new"
    private ArrayIndexOutOfBoundsException m_exception;

    /**
     * Constructor
     */
    LargeMessageExporter(final UnfinishedImExporterOperation p_unfinishedOperation) {
        m_unfinishedOperation = p_unfinishedOperation;
        m_exception = new ArrayIndexOutOfBoundsException();
    }

    @Override
    public String toString() {
        return "m_bufferAddress 0x" + Long.toHexString(m_bufferAddress) + ", m_bufferSize " + m_bufferSize + ", m_currentPosition " + m_currentPosition +
                ", m_startPosition " + m_startPosition + ", m_endPosition " + m_endPosition + ", m_skipBytes " + m_skipBytes + ", m_skippedBytes " +
                m_skippedBytes + ", m_unfinishedOperation " + m_unfinishedOperation;
    }

    @Override
    public int getNumberOfWrittenBytes() {
        if (m_currentPosition >= m_startPosition) {
            return m_currentPosition - m_startPosition + m_skipBytes;
        } else {
            return m_currentPosition + m_bufferSize - m_startPosition + m_skipBytes;
        }
    }

    @Override
    public void setPosition(final int p_position) {
        m_currentPosition = p_position;
        m_startPosition = p_position;
    }

    @Override
    public void setBuffer(final long p_addr, final int p_size) {
        m_bufferAddress = p_addr;
        m_bufferSize = p_size;
    }

    void setNumberOfWrittenBytes(final int p_writtenBytes) {
        m_skipBytes = p_writtenBytes;
        m_skippedBytes = 0;
    }

    void setLimit(final int p_limit) {
        m_endPosition = p_limit;
    }

    @Override
    public void exportObject(final Exportable p_object) {
        p_object.exportObject(this);
    }

    @Override
    public void writeBoolean(final boolean p_v) {
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Boolean was written before
            m_skippedBytes++;
        } else {
            if (m_currentPosition == m_bufferSize) {
                // Ring buffer overflow
                if (m_endPosition == 0) {
                    // Message overflow
                    // Not enough space in buffer currently -> abort
                    m_unfinishedOperation.setIndex(getNumberOfWrittenBytes());
                    throw m_exception;
                }
                // No message overflow
                UnsafeMemory.writeByte(m_bufferAddress, (byte) (p_v ? 1 : 0));
                m_currentPosition = 1;
            } else {
                // No ring buffer overflow
                if (m_currentPosition == m_endPosition) {
                    // Message overflow
                    // Not enough space in buffer currently -> abort
                    m_unfinishedOperation.setIndex(getNumberOfWrittenBytes());
                    throw m_exception;
                }
                // No message overflow
                UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v ? 1 : 0));
                m_currentPosition++;
            }
        }
    }

    @Override
    public void writeByte(final byte p_v) {
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Byte was written before
            m_skippedBytes++;
        } else {
            if (m_currentPosition == m_bufferSize) {
                // Ring buffer overflow
                if (m_endPosition == 0) {
                    // Message overflow
                    // Not enough space in buffer currently -> abort
                    m_unfinishedOperation.setIndex(getNumberOfWrittenBytes());
                    throw m_exception;
                }
                // No message overflow
                UnsafeMemory.writeByte(m_bufferAddress, p_v);
                m_currentPosition = 1;
            } else {
                // No ring buffer overflow
                if (m_currentPosition == m_endPosition) {
                    // Message overflow
                    // Not enough space in buffer currently -> abort
                    m_unfinishedOperation.setIndex(getNumberOfWrittenBytes());
                    throw m_exception;
                }
                // No message overflow
                UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, p_v);
                m_currentPosition++;
            }
        }
    }

    @Override
    public void writeShort(final short p_v) {
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Short was written before
            m_skippedBytes += Short.BYTES;
        } else {
            int bytesToSkip = 0;

            if (m_skippedBytes < m_skipBytes) {
                // Short was partly serialized -> skip already written bytes
                bytesToSkip += m_skipBytes - m_skippedBytes;
                m_skippedBytes = m_skipBytes;
            }

            if (m_currentPosition + Short.BYTES - bytesToSkip < m_bufferSize) {
                // No ring buffer overflow
                for (int i = bytesToSkip; i < Short.BYTES; i++) {
                    if (m_currentPosition == m_endPosition) {
                        // Message overflow
                        // Not enough space in buffer currently -> abort
                        m_unfinishedOperation.setIndex(getNumberOfWrittenBytes() - i);
                        throw m_exception;
                    }

                    // big endian to little endian
                    UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v >> i * 8 & 0xFF));
                    m_currentPosition++;
                }
            } else {
                // Ring buffer overflow
                // Write first part
                int i;
                for (i = bytesToSkip; i < Short.BYTES && m_currentPosition < m_bufferSize; i++) {
                    if (m_currentPosition == m_endPosition) {
                        // Message overflow
                        // Not enough space in buffer currently -> abort
                        m_unfinishedOperation.setIndex(getNumberOfWrittenBytes() - i);
                        throw m_exception;
                    }

                    // big endian to little endian
                    UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v >> i * 8 & 0xFF));
                    m_currentPosition++;
                }

                // Write second part
                m_currentPosition = 0;
                for (int j = i; j < Short.BYTES; j++) {
                    if (m_currentPosition == m_endPosition) {
                        // Message overflow
                        // Not enough space in buffer currently -> abort
                        m_unfinishedOperation.setIndex(getNumberOfWrittenBytes() - i);
                        throw m_exception;
                    }

                    // big endian to little endian
                    UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v >> j * 8 & 0xFF));
                    m_currentPosition++;
                }
            }
        }
    }

    @Override
    public void writeInt(final int p_v) {
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Int was written before
            m_skippedBytes += Integer.BYTES;
        } else {
            int bytesToSkip = 0;

            if (m_skippedBytes < m_skipBytes) {
                // Int was partly serialized -> skip already written bytes
                bytesToSkip += m_skipBytes - m_skippedBytes;
                m_skippedBytes = m_skipBytes;
            }

            if (m_currentPosition + Integer.BYTES - bytesToSkip < m_bufferSize) {
                // No ring buffer overflow
                for (int i = bytesToSkip; i < Integer.BYTES; i++) {
                    if (m_currentPosition == m_endPosition) {
                        // Message overflow
                        // Not enough space in buffer currently -> abort
                        m_unfinishedOperation.setIndex(getNumberOfWrittenBytes() - i);
                        throw m_exception;
                    }

                    // big endian to little endian
                    UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v >> i * 8 & 0xFF));
                    m_currentPosition++;
                }
            } else {
                // Ring buffer overflow
                // Write first part
                int i;
                for (i = bytesToSkip; i < Integer.BYTES && m_currentPosition < m_bufferSize; i++) {
                    if (m_currentPosition == m_endPosition) {
                        // Message overflow
                        // Not enough space in buffer currently -> abort
                        m_unfinishedOperation.setIndex(getNumberOfWrittenBytes() - i);
                        throw m_exception;
                    }

                    // big endian to little endian
                    UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v >> i * 8 & 0xFF));
                    m_currentPosition++;
                }

                // Write second part
                m_currentPosition = 0;
                for (int j = i; j < Integer.BYTES; j++) {
                    if (m_currentPosition == m_endPosition) {
                        // Message overflow
                        // Not enough space in buffer currently -> abort
                        m_unfinishedOperation.setIndex(getNumberOfWrittenBytes() - i);
                        throw m_exception;
                    }

                    // big endian to little endian
                    UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v >> j * 8 & 0xFF));
                    m_currentPosition++;
                }
            }
        }
    }

    @Override
    public void writeLong(final long p_v) {
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Long was written before
            m_skippedBytes += Long.BYTES;
        } else {
            int bytesToSkip = 0;

            if (m_skippedBytes < m_skipBytes) {
                // Long was partly serialized -> skip already written bytes
                bytesToSkip += m_skipBytes - m_skippedBytes;
                m_skippedBytes = m_skipBytes;
            }

            if (m_currentPosition + Long.BYTES - bytesToSkip < m_bufferSize) {
                // No ring buffer overflow
                for (int i = bytesToSkip; i < Long.BYTES; i++) {
                    if (m_currentPosition == m_endPosition) {
                        // Message overflow
                        // Not enough space in buffer currently -> abort
                        m_unfinishedOperation.setIndex(getNumberOfWrittenBytes() - i);
                        throw m_exception;
                    }

                    // big endian to little endian
                    UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v >> i * 8 & 0xFF));
                    m_currentPosition++;
                }
            } else {
                // Ring buffer overflow
                // Write first part
                int i;
                for (i = bytesToSkip; i < Long.BYTES && m_currentPosition < m_bufferSize; i++) {
                    if (m_currentPosition == m_endPosition) {
                        // Message overflow
                        // Not enough space in buffer currently -> abort
                        m_unfinishedOperation.setIndex(getNumberOfWrittenBytes() - i);
                        throw m_exception;
                    }

                    // big endian to little endian
                    UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v >> i * 8 & 0xFF));
                    m_currentPosition++;
                }

                // Write second part
                m_currentPosition = 0;
                for (int j = i; j < Long.BYTES; j++) {
                    if (m_currentPosition == m_endPosition) {
                        // Message overflow
                        // Not enough space in buffer currently -> abort
                        m_unfinishedOperation.setIndex(getNumberOfWrittenBytes() - i);
                        throw m_exception;
                    }

                    // big endian to little endian
                    UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v >> j * 8 & 0xFF));
                    m_currentPosition++;
                }
            }
        }
    }

    @Override
    public void writeFloat(final float p_v) {
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Float was written before
            m_skippedBytes += Float.BYTES;
        } else {
            writeInt(Float.floatToRawIntBits(p_v));
        }
    }

    @Override
    public void writeDouble(final double p_v) {
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Double was written before
            m_skippedBytes += Double.BYTES;
        } else {
            writeLong(Double.doubleToRawLongBits(p_v));
        }
    }

    @Override
    public void writeCompactNumber(final int p_v) {
        int length = ObjectSizeUtil.sizeofCompactedNumber(p_v);

        int i;
        int startPosition = m_currentPosition;
        if (m_skippedBytes < m_unfinishedOperation.getIndex() || m_skipBytes - m_skippedBytes >
                length /* special case: writing of an array was interrupted (-> skippedBytes == index), but not during writing of compact number */) {
            // Compact number was written before
            m_skippedBytes += length;
        } else {
            int bytesToSkip = 0;

            if (m_skippedBytes < m_skipBytes) {
                // Compact number was partly serialized -> continue
                bytesToSkip += m_skipBytes - m_skippedBytes;
            }

            try {
                for (i = bytesToSkip; i < length - 1; i++) {
                    writeByte((byte) ((byte) (p_v >> 7 * i) & 0x7F | 0x80));
                }
                writeByte((byte) ((byte) (p_v >> 7 * i) & 0x7F));
            } catch (final ArrayIndexOutOfBoundsException e) {
                // Not enough space in buffer currently -> abort
                m_unfinishedOperation.setIndex(startPosition - m_startPosition);
                throw e;
            }
        }
    }

    @Override
    public void writeString(final String p_str) {
        writeByteArray(p_str.getBytes());
    }

    @Override
    public int writeBytes(final byte[] p_array) {
        return writeBytes(p_array, 0, p_array.length);
    }

    @Override
    public int writeShorts(final short[] p_array) {
        return writeShorts(p_array, 0, p_array.length);
    }

    @Override
    public int writeInts(final int[] p_array) {
        return writeInts(p_array, 0, p_array.length);
    }

    @Override
    public int writeLongs(final long[] p_array) {
        return writeLongs(p_array, 0, p_array.length);
    }

    @Override
    public int writeBytes(final byte[] p_array, final int p_offset, final int p_length) {
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Byte array was read before
            m_skippedBytes += p_length;
        } else {
            int offset = p_offset;
            int bytesToWrite = p_length;

            if (m_skippedBytes < m_skipBytes) {
                // Byte array was partly serialized -> skip already written bytes
                offset += m_skipBytes - m_skippedBytes;
                bytesToWrite -= m_skipBytes - m_skippedBytes;
                m_skippedBytes = m_skipBytes;
            }

            if (m_currentPosition + bytesToWrite < m_bufferSize) {
                // No ring buffer overflow
                if (m_endPosition < m_currentPosition /* limit after ring buffer overflow -> irrelevant */ ||
                        m_currentPosition + bytesToWrite < m_endPosition) {
                    // No message overflow
                    int ret = UnsafeMemory.writeBytes(m_bufferAddress + m_currentPosition, p_array, offset, bytesToWrite);
                    m_currentPosition += Byte.BYTES * ret;
                } else {
                    // Message overflow
                    int ret = UnsafeMemory.writeBytes(m_bufferAddress + m_currentPosition, p_array, offset, m_endPosition - m_currentPosition);
                    // Not enough space in buffer currently -> abort
                    m_unfinishedOperation.setIndex(getNumberOfWrittenBytes());
                    m_currentPosition += Byte.BYTES * ret;
                    throw m_exception;
                }
            } else {
                // Ring buffer overflow
                if (m_endPosition < m_currentPosition /* limit after ring buffer overflow */) {
                    // No message overflow for first part
                    UnsafeMemory.writeBytes(m_bufferAddress + m_currentPosition, p_array, offset, m_bufferSize - m_currentPosition);
                } else {
                    // Message overflow
                    int ret = UnsafeMemory.writeBytes(m_bufferAddress + m_currentPosition, p_array, offset, m_endPosition - m_currentPosition);
                    // Not enough space in buffer currently -> abort
                    m_unfinishedOperation.setIndex(getNumberOfWrittenBytes());
                    m_currentPosition += Byte.BYTES * ret;
                    throw m_exception;
                }

                bytesToWrite -= m_bufferSize - m_currentPosition;
                if (bytesToWrite < m_endPosition) {
                    // No message overflow for second part
                    UnsafeMemory.writeBytes(m_bufferAddress, p_array, offset + m_bufferSize - m_currentPosition, bytesToWrite);
                } else {
                    // Message overflow
                    int ret = UnsafeMemory.writeBytes(m_bufferAddress, p_array, offset + m_bufferSize - m_currentPosition, m_endPosition);
                    // Not enough space in buffer currently -> abort
                    m_unfinishedOperation.setIndex(getNumberOfWrittenBytes());
                    m_currentPosition = Byte.BYTES * ret;
                    throw m_exception;
                }
                m_currentPosition = bytesToWrite;
            }
        }

        return p_length;
    }

    @Override
    public int writeShorts(final short[] p_array, final int p_offset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            writeShort(p_array[p_offset + i]);
        }

        return p_length;
    }

    @Override
    public int writeInts(final int[] p_array, final int p_offset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            writeInt(p_array[p_offset + i]);
        }

        return p_length;
    }

    @Override
    public int writeLongs(final long[] p_array, final int p_offset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            writeLong(p_array[p_offset + i]);
        }

        return p_length;
    }

    @Override
    public void writeByteArray(final byte[] p_array) {
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Array length and array were written before
            m_skippedBytes += ObjectSizeUtil.sizeofCompactedNumber(p_array.length) + p_array.length;
        } else {
            int startPosition = m_currentPosition + m_skippedBytes;
            writeCompactNumber(p_array.length);
            try {
                writeBytes(p_array);
            } catch (final ArrayIndexOutOfBoundsException e) {
                // Not enough space in buffer currently -> abort
                m_unfinishedOperation.setIndex(startPosition - m_startPosition);
                throw e;
            }
        }
    }

    @Override
    public void writeShortArray(final short[] p_array) {
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Array length and array were written before
            m_skippedBytes += ObjectSizeUtil.sizeofCompactedNumber(p_array.length) + p_array.length;
        } else {
            int startPosition = m_currentPosition;
            writeCompactNumber(p_array.length);
            try {
                writeShorts(p_array);
            } catch (final ArrayIndexOutOfBoundsException e) {
                // Not enough space in buffer currently -> abort
                m_unfinishedOperation.setIndex(startPosition - m_startPosition);
                throw e;
            }
        }
    }

    @Override
    public void writeIntArray(final int[] p_array) {
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Array length and array were written before
            m_skippedBytes += ObjectSizeUtil.sizeofCompactedNumber(p_array.length) + p_array.length;
        } else {
            int startPosition = m_currentPosition;
            writeCompactNumber(p_array.length);
            try {
                writeInts(p_array);
            } catch (final ArrayIndexOutOfBoundsException e) {
                // Not enough space in buffer currently -> abort
                m_unfinishedOperation.setIndex(startPosition - m_startPosition);
                throw e;
            }
        }
    }

    @Override
    public void writeLongArray(final long[] p_array) {
        if (m_skippedBytes < m_unfinishedOperation.getIndex()) {
            // Array length and array were written before
            m_skippedBytes += ObjectSizeUtil.sizeofCompactedNumber(p_array.length) + p_array.length;
        } else {
            int startPosition = m_currentPosition;
            writeCompactNumber(p_array.length);
            try {
                writeLongs(p_array);
            } catch (final ArrayIndexOutOfBoundsException e) {
                // Not enough space in buffer currently -> abort
                m_unfinishedOperation.setIndex(startPosition - m_startPosition);
                throw e;
            }
        }
    }
}
