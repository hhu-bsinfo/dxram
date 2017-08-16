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
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

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
        writeInt(Float.floatToIntBits(p_v));
    }

    @Override
    public void writeDouble(final double p_v) {
        writeLong(Double.doubleToLongBits(p_v));
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
    public boolean readBoolean(final boolean p_bool) {
        byte v = m_heap.readByte(m_allocatedMemoryStartAddress, m_offset);
        m_offset += Byte.BYTES;
        return v == 1;
    }

    @Override
    public byte readByte(final byte p_byte) {
        byte v = m_heap.readByte(m_allocatedMemoryStartAddress, m_offset);
        m_offset += Byte.BYTES;
        return v;
    }

    @Override
    public short readShort(final short p_short) {
        short v = m_heap.readShort(m_allocatedMemoryStartAddress, m_offset);
        m_offset += Short.BYTES;
        return v;
    }

    @Override
    public int readInt(final int p_int) {
        int v = m_heap.readInt(m_allocatedMemoryStartAddress, m_offset);
        m_offset += Integer.BYTES;
        return v;
    }

    @Override
    public long readLong(final long p_long) {
        long v = m_heap.readLong(m_allocatedMemoryStartAddress, m_offset);
        m_offset += Long.BYTES;
        return v;
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

        for (int i = 0; i < Integer.BYTES; i++) {
            int tmp = readByte((byte) 0);
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
    public byte[] readByteArray(final byte[] p_array) {
        byte[] arr = new byte[readCompactNumber(0)];
        readBytes(arr);
        return arr;
    }

    @Override
    public short[] readShortArray(final short[] p_array) {
        short[] arr = new short[readCompactNumber(0)];
        readShorts(arr);
        return arr;
    }

    @Override
    public int[] readIntArray(final int[] p_array) {
        int[] arr = new int[readCompactNumber(0)];
        readInts(arr);
        return arr;
    }

    @Override
    public long[] readLongArray(final long[] p_array) {
        long[] arr = new long[readCompactNumber(0)];
        readLongs(arr);
        return arr;
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
