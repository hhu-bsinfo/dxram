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

import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * Implementation of an Exporter for byte arrays.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.07.2017
 */
class NIOMessageExporter extends AbstractMessageExporter {

    private byte[] m_buffer;
    private int m_currentPosition;
    private int m_startPosition;

    /**
     * Constructor
     */
    NIOMessageExporter() {
    }

    @Override
    protected int getNumberOfWrittenBytes() {
        return m_currentPosition - m_startPosition;
    }

    @Override
    protected void setBuffer(final byte[] p_buffer) {
        m_buffer = p_buffer;
    }

    @Override
    protected void setPosition(final int p_position) {
        m_currentPosition = p_position;
        m_startPosition = p_position;
    }

    @Override
    public void exportObject(final Exportable p_object) {
        p_object.exportObject(this);
    }

    @Override
    public void writeBoolean(boolean p_v) {
        m_buffer[m_currentPosition++] = (byte) (p_v ? 1 : 0);
    }

    @Override
    public void writeByte(final byte p_v) {
        m_buffer[m_currentPosition++] = p_v;
    }

    @Override
    public void writeShort(final short p_v) {
        for (int i = 0; i < Short.BYTES; i++) {
            m_buffer[m_currentPosition++] = (byte) (p_v >> (Short.BYTES - 1 - i) * 8 & 0xFF);
        }
    }

    @Override
    public void writeInt(final int p_v) {
        for (int i = 0; i < Integer.BYTES; i++) {
            m_buffer[m_currentPosition++] = (byte) (p_v >> (Integer.BYTES - 1 - i) * 8 & 0xFF);
        }
    }

    @Override
    public void writeLong(final long p_v) {
        for (int i = 0; i < Long.BYTES; i++) {
            m_buffer[m_currentPosition++] = (byte) (p_v >> (Long.BYTES - 1 - i) * 8 & 0xFF);
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
    public void writeCompactNumber(int p_v) {
        int length = ObjectSizeUtil.sizeofCompactedNumber(p_v);

        int i;
        for (i = 0; i < length - 1; i++) {
            writeByte((byte) ((byte) (p_v >> 7 * i) & 0x7F | 0x80));
        }
        writeByte((byte) ((byte) (p_v >> 7 * i) & 0x7F));
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
    public int writeBytes(final byte[] p_array, final int p_offset, final int p_length) {
        System.arraycopy(p_array, p_offset, m_buffer, m_currentPosition, p_length);
        m_currentPosition += p_length;

        return p_length;
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

}
