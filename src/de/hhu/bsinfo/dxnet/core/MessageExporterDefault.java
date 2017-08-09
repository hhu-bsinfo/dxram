package de.hhu.bsinfo.dxnet.core;

import java.nio.charset.StandardCharsets;

import de.hhu.bsinfo.utils.UnsafeMemory;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * Implementation of an Exporter for network messages.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.07.2017
 */
class MessageExporterDefault extends AbstractMessageExporter {

    private long m_bufferAddress;
    private int m_currentPosition;
    private int m_startPosition;

    /**
     * Constructor
     */
    MessageExporterDefault() {

    }

    @Override
    public String toString() {
        return "m_bufferAddress 0x" + Long.toHexString(m_bufferAddress) + ", m_currentPosition " + m_currentPosition + ", m_startPosition " + m_startPosition;
    }

    @Override
    public int getNumberOfWrittenBytes() {
        return m_currentPosition - m_startPosition;
    }

    @Override
    public void setPosition(final int p_position) {
        m_currentPosition = p_position;
        m_startPosition = p_position;
    }

    @Override
    public void setBuffer(final long p_addr, final int p_size) {
        m_bufferAddress = p_addr;
    }

    @Override
    public void exportObject(final Exportable p_object) {
        p_object.exportObject(this);
    }

    @Override
    public void writeBoolean(final boolean p_v) {
        UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, (byte) (p_v ? 1 : 0));
        m_currentPosition++;
    }

    @Override
    public void writeByte(final byte p_v) {
        UnsafeMemory.writeByte(m_bufferAddress + m_currentPosition, p_v);
        m_currentPosition++;
    }

    @Override
    public void writeShort(final short p_v) {
        UnsafeMemory.writeShort(m_bufferAddress + m_currentPosition, p_v);
        m_currentPosition += Short.BYTES;
    }

    @Override
    public void writeInt(final int p_v) {
        UnsafeMemory.writeInt(m_bufferAddress + m_currentPosition, p_v);
        m_currentPosition += Integer.BYTES;
    }

    @Override
    public void writeLong(final long p_v) {
        UnsafeMemory.writeLong(m_bufferAddress + m_currentPosition, p_v);
        m_currentPosition += Long.BYTES;
    }

    @Override
    public void writeFloat(final float p_v) {
        UnsafeMemory.writeFloat(m_bufferAddress + m_currentPosition, p_v);
        m_currentPosition += Float.BYTES;
    }

    @Override
    public void writeDouble(final double p_v) {
        UnsafeMemory.writeDouble(m_bufferAddress + m_currentPosition, p_v);
        m_currentPosition += Double.BYTES;
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
        writeByteArray(p_str.getBytes(StandardCharsets.US_ASCII));
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
        int ret = UnsafeMemory.writeBytes(m_bufferAddress + m_currentPosition, p_array, p_offset, p_length);
        m_currentPosition += Byte.BYTES * ret;

        return ret;
    }

    @Override
    public int writeShorts(final short[] p_array, final int p_offset, final int p_length) {
        int ret = UnsafeMemory.writeShorts(m_bufferAddress + m_currentPosition, p_array, p_offset, p_length);
        m_currentPosition += Short.BYTES * ret;

        return ret;
    }

    @Override
    public int writeInts(final int[] p_array, final int p_offset, final int p_length) {
        int ret = UnsafeMemory.writeInts(m_bufferAddress + m_currentPosition, p_array, p_offset, p_length);
        m_currentPosition += Integer.BYTES * ret;

        return ret;
    }

    @Override
    public int writeLongs(final long[] p_array, final int p_offset, final int p_length) {
        int ret = UnsafeMemory.writeLongs(m_bufferAddress + m_currentPosition, p_array, p_offset, p_length);
        m_currentPosition += Long.BYTES * ret;

        return ret;
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

}
