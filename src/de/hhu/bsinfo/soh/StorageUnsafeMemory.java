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

package de.hhu.bsinfo.soh;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import sun.misc.Unsafe;

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
    private static final Unsafe UNSAFE = UnsafeHandler.getInstance().getUnsafe();

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
    public void dump(final File p_file, final long p_ptr, final long p_length) {
        assert p_ptr >= 0;
        assert p_ptr < m_memorySize;
        assert p_ptr + p_length <= m_memorySize;

        RandomAccessFile outFile = null;
        try {
            outFile = new RandomAccessFile(p_file, "rw");

            long offset = 0;
            while (offset < p_length) {
                outFile.writeByte(UNSAFE.getByte(m_memoryBase + p_ptr + offset));
                offset++;
            }
        } catch (final IOException e) {
            throw new MemoryRuntimeException(e.getMessage());
        } finally {
            try {
                if (outFile != null) {
                    outFile.close();
                }
            } catch (final IOException ignored) {
            }
        }
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
