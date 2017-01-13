/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.data;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Implementation of an Importer/Exporter for DataStructure objects for
 * network messages. Use this if a network message has to send an object,
 * which implements the DataStructure interface.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class MessagesDataStructureImExporter implements Importer, Exporter {

    private ByteBuffer m_messageBuffer;
    private int m_payloadSize;

    /**
     * Constructor
     *
     * @param p_messageBuffer
     *     Buffer the message has to write to/read from.
     */
    public MessagesDataStructureImExporter(final ByteBuffer p_messageBuffer) {
        m_messageBuffer = p_messageBuffer;
    }

    /**
     * Get the payload size that was set previously indicating the total size of the data structure.
     *
     * @return Payload size.
     */
    int getPayloadSize() {
        return m_payloadSize;
    }

    /**
     * Set the size of the payload to analyze when importing an object
     * (for dynamic sized objects) or the amount of bytes in the buffer available
     * when exporting an object.
     * Use case: Generic chunk data with dynamic size (see Chunk object).
     *
     * @param p_size
     *     Payload size to set.
     */
    public void setPayloadSize(final int p_size) {
        m_payloadSize = p_size;
    }

    @Override
    public void exportObject(final Exportable p_object) {
        p_object.exportObject(this);
    }

    @Override
    public void writeByte(final byte p_v) {
        m_messageBuffer.put(p_v);
    }

    @Override
    public void writeShort(final short p_v) {
        m_messageBuffer.putShort(p_v);
    }

    @Override
    public void writeInt(final int p_v) {
        m_messageBuffer.putInt(p_v);
    }

    @Override
    public void writeLong(final long p_v) {
        m_messageBuffer.putLong(p_v);
    }

    @Override
    public void writeFloat(final float p_v) {
        m_messageBuffer.putFloat(p_v);
    }

    @Override
    public void writeDouble(final double p_v) {
        m_messageBuffer.putDouble(p_v);
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
        m_messageBuffer.put(p_array, p_offset, p_length);
        return p_length;
    }

    @Override
    public void importObject(final Importable p_object) {
        p_object.importObject(this);
    }

    @Override
    public byte readByte() {
        return m_messageBuffer.get();
    }

    @Override
    public short readShort() {
        return m_messageBuffer.getShort();
    }

    @Override
    public int readInt() {
        return m_messageBuffer.getInt();
    }

    @Override
    public long readLong() {
        return m_messageBuffer.getLong();
    }

    @Override
    public float readFloat() {
        return m_messageBuffer.getFloat();
    }

    @Override
    public double readDouble() {
        return m_messageBuffer.getDouble();
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
        m_messageBuffer.get(p_array, p_offset, p_length);
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
            m_messageBuffer.putShort(p_array[p_offset + i]);
        }

        return p_length;
    }

    @Override
    public int writeInts(final int[] p_array, final int p_offset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            m_messageBuffer.putInt(p_array[p_offset + i]);
        }

        return p_length;
    }

    @Override
    public int writeLongs(final long[] p_array, final int p_offset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            m_messageBuffer.putLong(p_array[p_offset + i]);
        }

        return p_length;
    }

    @Override
    public void writeByteArray(final byte[] p_array) {
        writeInt(p_array.length);
        writeBytes(p_array);
    }

    @Override
    public void writeShortArray(final short[] p_array) {
        writeInt(p_array.length);
        writeShorts(p_array);
    }

    @Override
    public void writeIntArray(final int[] p_array) {
        writeInt(p_array.length);
        writeInts(p_array);
    }

    @Override
    public void writeLongArray(final long[] p_array) {
        writeInt(p_array.length);
        writeLongs(p_array);
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
            p_array[p_offset + i] = m_messageBuffer.getShort();
        }

        return p_length;
    }

    @Override
    public int readInts(final int[] p_array, final int p_offset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            p_array[p_offset + i] = m_messageBuffer.getInt();
        }

        return p_length;
    }

    @Override
    public int readLongs(final long[] p_array, final int p_offset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            p_array[p_offset + i] = m_messageBuffer.getLong();
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

}
