package de.hhu.bsinfo.net.ib;

import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.utils.UnsafeMemory;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * Created by nothaas on 7/11/17.
 */
class IBMessageExporterOverflow extends AbstractMessageExporter {

    private long m_bufferAddress;
    private int m_bufferSize;
    private int m_currentPosition;
    private int m_startPosition;

    /**
     * Constructor
     */
    IBMessageExporterOverflow() {

    }

    @Override
    public int getNumberOfWrittenBytes() {
        return m_bufferSize - m_startPosition + m_currentPosition;
    }

    @Override
    public void setPosition(final int p_position) {
        m_currentPosition = p_position;
        m_startPosition = p_position;
    }

    @Override
    public void exportObject(final Exportable p_object) {
        p_object.exportObject(this);
    }

    @Override
    public void writeBoolean(final boolean p_v) {
        if (m_currentPosition == m_bufferSize) {
            m_currentPosition = 0;
            UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v ? 1 : 0));
            m_currentPosition++;
        } else {
            UnsafeMemory.writeByte(m_bufferAddress, (byte) (p_v ? 1 : 0));
            m_currentPosition++;
        }
    }

    @Override
    public void writeByte(final byte p_v) {
        if (m_currentPosition == m_bufferSize) {
            m_currentPosition = 0;
            UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, p_v);
            m_currentPosition++;
        } else {
            UnsafeMemory.writeByte(m_bufferAddress, p_v);
            m_currentPosition++;
        }
    }

    @Override
    public void writeShort(final short p_v) {
        if (m_currentPosition + Short.BYTES < m_bufferSize) {
            UnsafeMemory.writeShort(m_bufferAddress + m_currentPosition, p_v);
            m_currentPosition += Short.BYTES;
        } else {
            int i;
            for (i = 0; i < 2 && m_currentPosition < m_bufferSize; i++) {
                // big endian to little endian
                UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v >> i * 8 & 0xFF));
                m_currentPosition++;
            }
            m_currentPosition = 0;
            for (int j = i; j < Short.BYTES; j++) {
                // big endian to little endian
                UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v >> j * 8 & 0xFF));
                m_currentPosition++;
            }
        }
    }

    @Override
    public void writeInt(final int p_v) {
        if (m_currentPosition + Integer.BYTES < m_bufferSize) {
            UnsafeMemory.writeInt(m_bufferAddress + m_currentPosition, p_v);
            m_currentPosition += Integer.BYTES;
        } else {
            int i;
            for (i = 0; i < Integer.BYTES && m_currentPosition < m_bufferSize; i++) {
                // big endian to little endian
                UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v >> i * 8 & 0xFF));
                m_currentPosition++;
            }
            m_currentPosition = 0;
            for (int j = i; j < Integer.BYTES; j++) {
                // big endian to little endian
                UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v >> j * 8 & 0xFF));
                m_currentPosition++;
            }
        }
    }

    @Override
    public void writeLong(final long p_v) {
        if (m_currentPosition + Long.BYTES < m_bufferSize) {
            UnsafeMemory.writeLong(m_bufferAddress + m_currentPosition, p_v);
            m_currentPosition += Long.BYTES;
        } else {
            int i;
            for (i = 0; i < Long.BYTES && m_currentPosition < m_bufferSize; i++) {
                // big endian to little endian
                UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v >> i * 8 & 0xFF));
                m_currentPosition++;
            }
            m_currentPosition = 0;
            for (int j = i; j < Long.BYTES; j++) {
                // big endian to little endian
                UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v >> j * 8 & 0xFF));
                m_currentPosition++;
            }
        }
    }

    @Override
    public void writeFloat(final float p_v) {
        writeInt(Float.floatToRawIntBits(p_v));
    }

    @Override
    public void writeDouble(final double p_v) {
        writeLong(Double.doubleToRawLongBits(p_v));
    }

    @Override
    public void writeCompactNumber(final int p_v) {
        int length = ObjectSizeUtil.sizeofCompactedNumber(p_v);

        int i;
        for (i = 0; i < length - 1; i++) {
            UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) ((byte) (p_v >> 7 * i) & 0x7F | 0x80));
            m_currentPosition++;
        }
        UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) ((byte) (p_v >> 7 * i) & 0x7F));
        m_currentPosition++;
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
        if (m_currentPosition + p_length < m_bufferSize) {
            int ret = UnsafeMemory.writeBytes(m_bufferAddress + m_currentPosition, p_array, p_offset, p_length);
            m_currentPosition += Byte.BYTES * ret;
        } else {
            UnsafeMemory.writeBytes(m_bufferAddress + m_currentPosition, p_array, p_offset, m_bufferSize - m_currentPosition);
            UnsafeMemory.writeBytes(m_bufferAddress + m_currentPosition, p_array, p_offset + m_bufferSize - m_currentPosition,
                    p_length - (m_bufferSize - m_currentPosition));
            m_currentPosition = p_length - (m_bufferSize - m_currentPosition) + 1;
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
        writeCompactNumber(p_array.length);
        writeBytes(p_array);
    }

    @Override
    public void writeShortArray(final short[] p_array) {
        writeCompactNumber(p_array.length);
        writeShorts(p_array);
    }

    @Override
    public void writeIntArray(final int[] p_array) {
        writeCompactNumber(p_array.length);
        writeInts(p_array);
    }

    @Override
    public void writeLongArray(final long[] p_array) {
        writeCompactNumber(p_array.length);
        writeLongs(p_array);
    }

    @Override
    public void writeStringArray(final String[] p_array) {
        writeCompactNumber(p_array.length);

        for (int i = 0; i < p_array.length; i++) {
            writeString(p_array[i]);
        }
    }

    void setBuffer(final long p_addr, final int p_size) {
        m_bufferAddress = p_addr;
        m_bufferSize = p_size;
    }
}
