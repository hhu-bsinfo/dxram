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

import de.hhu.bsinfo.utils.UnsafeHandler;

/**
 * Implementation of a storage based on an unsafe allocated
 * block of memory.
 * Note: This class is deprecated (replaced by JNINativeMemory),
 * due to inefficiency and endianess bugs/issues.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 */
public class StorageUnsafeMemory implements Storage {
    @SuppressWarnings("sunapi")
    private static final sun.misc.Unsafe UNSAFE = UnsafeHandler.getInstance().getUnsafe();

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
            m_memoryBase = UNSAFE.allocateMemory(p_size);
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
            UNSAFE.freeMemory(m_memoryBase);
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

        UNSAFE.setMemory(m_memoryBase + p_ptr, p_size, p_value);
    }

    @Override
    public int readBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Byte.BYTES * p_length);

        for (int i = 0; i < p_length; i++) {
            p_array[p_arrayOffset + i] = UNSAFE.getByte(m_memoryBase + p_ptr + i);
        }

        return p_length;
    }

    @Override
    public int readShorts(long p_ptr, short[] p_array, int p_arrayOffset, int p_length) {
        assert assertMemoryBounds(p_ptr, Short.BYTES * p_length);

        for (int i = 0; i < p_length; i++) {
            p_array[i + p_arrayOffset] = UNSAFE.getShort(m_memoryBase + p_ptr + i * Short.BYTES);
        }

        return p_length;
    }

    @Override
    public int readInts(long p_ptr, int[] p_array, int p_arrayOffset, int p_length) {
        assert assertMemoryBounds(p_ptr, Integer.BYTES * p_length);

        for (int i = 0; i < p_length; i++) {
            p_array[i + p_arrayOffset] = UNSAFE.getInt(m_memoryBase + p_ptr + i * Integer.BYTES);
        }

        return p_length;
    }

    @Override
    public int readLongs(long p_ptr, long[] p_array, int p_arrayOffset, int p_length) {
        assert assertMemoryBounds(p_ptr, Long.BYTES * p_length);

        for (int i = 0; i < p_length; i++) {
            p_array[i + p_arrayOffset] = UNSAFE.getLong(m_memoryBase + p_ptr + i * Long.BYTES);
        }

        return p_length;
    }

    @Override
    public byte readByte(final long p_ptr) {
        assert assertMemoryBounds(p_ptr, Byte.BYTES);

        return UNSAFE.getByte(m_memoryBase + p_ptr);
    }

    @Override
    public short readShort(final long p_ptr) {
        assert assertMemoryBounds(p_ptr, Short.BYTES);

        return UNSAFE.getShort(m_memoryBase + p_ptr);
    }

    @Override
    public int readInt(final long p_ptr) {
        assert assertMemoryBounds(p_ptr, Integer.BYTES);

        return UNSAFE.getInt(m_memoryBase + p_ptr);
    }

    @Override
    public long readLong(final long p_ptr) {
        assert assertMemoryBounds(p_ptr, Long.BYTES);

        return UNSAFE.getLong(m_memoryBase + p_ptr);
    }

    @Override
    public int writeBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) {
        assert assertMemoryBounds(p_ptr, Byte.BYTES * p_length);

        for (int i = 0; i < p_length; i++) {
            UNSAFE.putByte(m_memoryBase + p_ptr + i, p_array[p_arrayOffset + i]);
        }

        return p_length;
    }

    @Override
    public int writeShorts(long p_ptr, short[] p_array, int p_arrayOffset, int p_length) {
        assert assertMemoryBounds(p_ptr, Short.BYTES * p_length);

        for (int i = 0; i < p_length; i++) {
            UNSAFE.putShort(m_memoryBase + p_ptr + i * Short.BYTES, p_array[i + p_arrayOffset]);
        }

        return p_length;
    }

    @Override
    public int writeInts(long p_ptr, int[] p_array, int p_arrayOffset, int p_length) {
        assert assertMemoryBounds(p_ptr, Integer.BYTES * p_length);

        for (int i = 0; i < p_length; i++) {
            UNSAFE.putInt(m_memoryBase + p_ptr + i * Integer.BYTES, p_array[i + p_arrayOffset]);
        }

        return p_length;
    }

    @Override
    public int writeLongs(long p_ptr, long[] p_array, int p_arrayOffset, int p_length) {
        assert assertMemoryBounds(p_ptr, Long.BYTES * p_length);

        for (int i = 0; i < p_length; i++) {
            UNSAFE.putLong(m_memoryBase + p_ptr + i * Long.BYTES, p_array[i + p_arrayOffset]);
        }

        return p_length;
    }

    @Override
    public void writeByte(final long p_ptr, final byte p_value) {
        assert assertMemoryBounds(p_ptr, Byte.BYTES);

        UNSAFE.putByte(m_memoryBase + p_ptr, p_value);
    }

    @Override
    public void writeShort(final long p_ptr, final short p_value) {
        assert assertMemoryBounds(p_ptr, Short.BYTES);

        UNSAFE.putShort(m_memoryBase + p_ptr, p_value);
    }

    @Override
    public void writeInt(final long p_ptr, final int p_value) {
        assert assertMemoryBounds(p_ptr, Integer.BYTES);

        UNSAFE.putInt(m_memoryBase + p_ptr, p_value);
    }

    @Override
    public void writeLong(final long p_ptr, final long p_value) {
        assert assertMemoryBounds(p_ptr, Long.BYTES);

        UNSAFE.putLong(m_memoryBase + p_ptr, p_value);
    }

    @Override
    public long readVal(final long p_ptr, final int p_count) {
        assert assertMemoryBounds(p_ptr, p_count);

        long val = 0;

        for (int i = 0; i < p_count; i++) {
            // kill the sign by & 0xFF
            val |= (long) (UNSAFE.getByte(m_memoryBase + p_ptr + i) & 0xFF) << 8 * i;
        }

        return val;
    }

    @Override
    public void writeVal(final long p_ptr, final long p_val, final int p_count) {
        assert assertMemoryBounds(p_ptr, p_count);

        for (int i = 0; i < p_count; i++) {
            UNSAFE.putByte(m_memoryBase + p_ptr + i, (byte) (p_val >> 8 * i & 0xFF));
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
