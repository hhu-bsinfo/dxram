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

package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.soh.SmallObjectHeap;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Importer/Exporter wrapper to allow Importables/Exportables to be directly written
 * to the SmallObjectHeap.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.03.2016
 */
class SmallObjectHeapDataStructureImExporter implements Importer, Exporter {

    private SmallObjectHeap m_heap;
    private long m_allocatedMemoryStartAddress = -1;
    private int m_offset = -1;

    /**
     * Constructor
     *
     * @param p_heap
     *         The heap to access for the importer/exporter.
     * @param p_allocatedMemoryStartAddress
     *         The start address of the allocated memory block to access.
     * @param p_offset
     *         The start offset within the allocated block.
     */
    SmallObjectHeapDataStructureImExporter(final SmallObjectHeap p_heap, final long p_allocatedMemoryStartAddress, final int p_offset) {
        m_heap = p_heap;
        m_allocatedMemoryStartAddress = p_allocatedMemoryStartAddress;
        m_offset = p_offset;
    }

    /**
     * Sets the start address
     *
     * @param p_allocatedMemoryStartAddress
     *         the start address
     */
    void setAllocatedMemoryStartAddress(final long p_allocatedMemoryStartAddress) {
        m_allocatedMemoryStartAddress = p_allocatedMemoryStartAddress;
    }

    /**
     * Sets the offset
     *
     * @param p_offset
     *         the offset
     */
    public void setOffset(final int p_offset) {
        m_offset = p_offset;
    }

    @Override
    public void exportObject(final Exportable p_object) {
        p_object.exportObject(this);
    }

    @Override
    public void writeBoolean(boolean p_v) {
        m_heap.writeByte(m_allocatedMemoryStartAddress, m_offset, (byte) (p_v ? 1 : 0));
        m_offset += Byte.BYTES;
    }

    @Override
    public void writeByte(final byte p_v) {
        m_heap.writeByte(m_allocatedMemoryStartAddress, m_offset, p_v);
        m_offset += Byte.BYTES;
    }

    @Override
    public void writeShort(final short p_v) {
        m_heap.writeShort(m_allocatedMemoryStartAddress, m_offset, p_v);
        m_offset += Short.BYTES;
    }

    @Override
    public void writeInt(final int p_v) {
        m_heap.writeInt(m_allocatedMemoryStartAddress, m_offset, p_v);
        m_offset += Integer.BYTES;
    }

    @Override
    public void writeLong(final long p_v) {
        m_heap.writeLong(m_allocatedMemoryStartAddress, m_offset, p_v);
        m_offset += Long.BYTES;
    }

    @Override
    public void writeFloat(final float p_v) {
        throw new RuntimeException("Not supported.");
    }

    @Override
    public void writeDouble(final double p_v) {
        throw new RuntimeException("Not supported.");
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
        int written = m_heap.writeBytes(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
        if (written != -1) {
            m_offset += written * Byte.BYTES;
        }
        return written;
    }

    @Override
    public void importObject(final Importable p_object) {
        p_object.importObject(this);
    }

    @Override
    public boolean readBoolean() {
        byte v = m_heap.readByte(m_allocatedMemoryStartAddress, m_offset);
        m_offset += Byte.BYTES;
        return v == 1;
    }

    @Override
    public byte readByte() {
        byte v = m_heap.readByte(m_allocatedMemoryStartAddress, m_offset);
        m_offset += Byte.BYTES;
        return v;
    }

    @Override
    public short readShort() {
        short v = m_heap.readShort(m_allocatedMemoryStartAddress, m_offset);
        m_offset += Short.BYTES;
        return v;
    }

    @Override
    public int readInt() {
        int v = m_heap.readInt(m_allocatedMemoryStartAddress, m_offset);
        m_offset += Integer.BYTES;
        return v;
    }

    @Override
    public long readLong() {
        long v = m_heap.readLong(m_allocatedMemoryStartAddress, m_offset);
        m_offset += Long.BYTES;
        return v;
    }

    @Override
    public float readFloat() {
        throw new RuntimeException("Not supported.");
    }

    @Override
    public double readDouble() {
        throw new RuntimeException("Not supported.");
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
        int read = m_heap.readBytes(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
        if (read != -1) {
            m_offset += read * Byte.BYTES;
        }
        return read;
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

    @Override
    public String[] readStringArray() {
        String[] str = new String[readInt()];

        for (int i = 0; i < str.length; i++) {
            str[i] = readString();
        }

        return str;
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
        int written = m_heap.writeShorts(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
        if (written != -1) {
            m_offset += written * Short.BYTES;
        }
        return written;
    }

    @Override
    public int writeInts(final int[] p_array, final int p_offset, final int p_length) {
        int written = m_heap.writeInts(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
        if (written != -1) {
            m_offset += written * Integer.BYTES;
        }
        return written;
    }

    @Override
    public int writeLongs(final long[] p_array, final int p_offset, final int p_length) {
        int written = m_heap.writeLongs(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
        if (written != -1) {
            m_offset += written * Long.BYTES;
        }
        return written;
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
    public void writeStringArray(final String[] p_array) {
        writeInt(p_array.length);

        for (int i = 0; i < p_array.length; i++) {
            writeString(p_array[i]);
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
        int read = m_heap.readShorts(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
        if (read != -1) {
            m_offset += read * Short.BYTES;
        }
        return read;
    }

    @Override
    public int readInts(final int[] p_array, final int p_offset, final int p_length) {
        int read = m_heap.readInts(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
        if (read != -1) {
            m_offset += read * Integer.BYTES;
        }
        return read;
    }

    @Override
    public int readLongs(final long[] p_array, final int p_offset, final int p_length) {
        int read = m_heap.readLongs(m_allocatedMemoryStartAddress, m_offset, p_array, p_offset, p_length);
        if (read != -1) {
            m_offset += read * Long.BYTES;
        }
        return read;
    }
}
