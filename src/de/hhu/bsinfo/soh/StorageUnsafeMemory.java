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

package de.hhu.bsinfo.soh;

import de.hhu.bsinfo.utils.UnsafeMemory;

/**
 * Implementation of a storage based on an unsafe allocated
 * block of memory
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 */
public class StorageUnsafeMemory implements Storage {
    private long m_memoryBase = -1;
    private long m_memorySize = -1;

    /**
     * Default constructor
     */
    public StorageUnsafeMemory() {

    }

    @Override
    public long getSize() {
        return m_memorySize;
    }

    @Override
    public void allocate(final long p_size) {
        assert p_size > 0;

        try {
            m_memoryBase = UnsafeMemory.allocate(p_size);
        } catch (final Throwable e) {
            throw new MemoryRuntimeException("Could not initialize memory", e);
        }

        m_memorySize = p_size;
    }

    @Override
    public void free() {
        if (m_memoryBase == -1) {
            return;
        }

        try {
            UnsafeMemory.free(m_memoryBase);
        } catch (final Throwable e) {
            throw new MemoryRuntimeException("Could not free memory", e);
        }

        m_memorySize = 0;
    }

    @Override
    public String toString() {
        return "m_memoryBase=0x" + Long.toHexString(m_memoryBase) + ", m_memorySize: " + m_memorySize;
    }

    @Override
    public void set(final long p_ptr, final long p_size, final byte p_value) {
        assert assertMemoryBounds(p_ptr, Byte.BYTES * p_size);

        UnsafeMemory.set(m_memoryBase + p_ptr, p_size, p_value);
    }

    @Override
    public int readBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Byte.BYTES * p_length);

        return UnsafeMemory.readBytes(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    @Override
    public int readShorts(final long p_ptr, final short[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Short.BYTES * p_length);

        return UnsafeMemory.readShorts(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    @Override
    public int readInts(final long p_ptr, final int[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Integer.BYTES * p_length);

        return UnsafeMemory.readInts(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    @Override
    public int readLongs(final long p_ptr, final long[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Long.BYTES * p_length);

        return UnsafeMemory.readLongs(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    @Override
    public byte readByte(final long p_ptr) {
        assert assertMemoryBounds(p_ptr, Byte.BYTES);

        return UnsafeMemory.readByte(m_memoryBase + p_ptr);
    }

    @Override
    public short readShort(final long p_ptr) {
        assert assertMemoryBounds(p_ptr, Short.BYTES);

        return UnsafeMemory.readShort(m_memoryBase + p_ptr);
    }

    @Override
    public int readInt(final long p_ptr) {
        assert assertMemoryBounds(p_ptr, Integer.BYTES);

        return UnsafeMemory.readInt(m_memoryBase + p_ptr);
    }

    @Override
    public long readLong(final long p_ptr) {
        assert assertMemoryBounds(p_ptr, Long.BYTES);

        return UnsafeMemory.readLong(m_memoryBase + p_ptr);
    }

    @Override
    public int writeBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Byte.BYTES * p_length);

        return UnsafeMemory.writeBytes(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    @Override
    public int writeShorts(final long p_ptr, final short[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Short.BYTES * p_length);

        return UnsafeMemory.writeShorts(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    @Override
    public int writeInts(final long p_ptr, final int[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Integer.BYTES * p_length);

        return UnsafeMemory.writeInts(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    @Override
    public int writeLongs(final long p_ptr, final long[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Long.BYTES * p_length);

        return UnsafeMemory.writeLongs(m_memoryBase + p_ptr, p_array, p_arrayOffset, p_length);
    }

    @Override
    public void writeByte(final long p_ptr, final byte p_value) {
        assert assertMemoryBounds(p_ptr, Byte.BYTES);

        UnsafeMemory.writeByte(m_memoryBase + p_ptr, p_value);
    }

    @Override
    public void writeShort(final long p_ptr, final short p_value) {
        assert assertMemoryBounds(p_ptr, Short.BYTES);

        UnsafeMemory.writeShort(m_memoryBase + p_ptr, p_value);
    }

    @Override
    public void writeInt(final long p_ptr, final int p_value) {
        assert assertMemoryBounds(p_ptr, Integer.BYTES);

        UnsafeMemory.writeInt(m_memoryBase + p_ptr, p_value);
    }

    @Override
    public void writeLong(final long p_ptr, final long p_value) {
        assert assertMemoryBounds(p_ptr, Long.BYTES);

        UnsafeMemory.writeLong(m_memoryBase + p_ptr, p_value);
    }

    @Override
    public long readVal(final long p_ptr, final int p_count) {
        assert assertMemoryBounds(p_ptr, p_count);

        long val = 0;

        for (int i = 0; i < p_count; i++) {
            // kill the sign by & 0xFF
            val |= (long) (UnsafeMemory.readByte(m_memoryBase + p_ptr + i) & 0xFF) << 8 * i;
        }

        return val;
    }

    @Override
    public void writeVal(final long p_ptr, final long p_val, final int p_count) {
        assert assertMemoryBounds(p_ptr, p_count);

        for (int i = 0; i < p_count; i++) {
            UnsafeMemory.writeByte(m_memoryBase + p_ptr + i, (byte) (p_val >> 8 * i & 0xFF));
        }
    }

    private boolean assertMemoryBounds(final long p_ptr, final long p_length) {
        if (p_ptr < 0) {
            throw new MemoryRuntimeException("Pointer is negative " + p_ptr);
        }

        if (p_ptr + p_length > m_memorySize || p_ptr + p_length < 0) {
            throw new MemoryRuntimeException(
                    "Accessing memory at " + p_ptr + ", length " + p_length + " out of bounds: base " + m_memoryBase + ", size " + m_memorySize);
        }

        return true;
    }
}
