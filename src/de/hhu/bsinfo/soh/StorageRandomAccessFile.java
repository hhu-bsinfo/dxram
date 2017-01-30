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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

/**
 * Implementation of a storage based on a random access file.
 * Important note: This is quite slow and will take up
 * as much disk space as memory is requested on initialization.
 * Used for testing/memory debugging only.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 */
public class StorageRandomAccessFile implements Storage {
    private RandomAccessFile m_file;
    private long m_size;

    /**
     * Constructor
     *
     * @param p_file
     *     File to use as storage.
     * @throws FileNotFoundException
     *     If creating random access file failed.
     */
    public StorageRandomAccessFile(final File p_file) throws FileNotFoundException {
        m_file = new RandomAccessFile(p_file, "rwd");
    }

    @Override
    public void allocate(final long p_size) {
        final byte[] buf = new byte[8192];
        long size = p_size;

        try {
            m_file.setLength(0);
            m_file.seek(0);

            while (size > 0) {
                if (size >= buf.length) {
                    m_file.write(buf, 0, buf.length);
                    size -= buf.length;
                } else {
                    m_file.write(buf, 0, (int) size);
                    size = 0;
                }
            }

            m_size = m_file.length();
        } catch (final IOException e) {
            throw new MemoryRuntimeException("Could not initialize memory", e);
        }
    }

    @Override
    public void free() {
        try {
            m_file.close();
        } catch (final IOException e) {
            throw new MemoryRuntimeException("Could not free memory", e);
        }
    }

    @Override
    public long getSize() {
        return m_size;
    }

    @Override
    public void set(final long p_ptr, final long p_size, final byte p_value) {
        assert p_ptr >= 0;
        assert p_ptr < m_size;
        assert p_ptr + p_size <= m_size;

        final byte[] buf = new byte[8192];
        Arrays.fill(buf, p_value);
        long size = p_size;

        try {
            m_file.seek(p_ptr);

            while (size > 0) {
                if (size >= buf.length) {
                    m_file.write(buf, 0, buf.length);
                    size -= buf.length;
                } else {
                    m_file.write(buf, 0, (int) size);
                    size = 0;
                }
            }

            m_size = m_file.length();
        } catch (final IOException e) {
            throw new MemoryRuntimeException("Could not set memory", e);
        }
    }

    @Override
    public int readBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) {
        assert p_ptr >= 0;
        assert p_ptr < m_size;
        assert p_ptr + p_length <= m_size;

        int bytesRead;

        try {
            m_file.seek(p_ptr);
            bytesRead = m_file.read(p_array, p_arrayOffset, p_length);
        } catch (final IOException e) {
            throw new MemoryRuntimeException("reading bytes failed " + e);
        }

        return bytesRead;
    }

    @Override
    public byte readByte(final long p_ptr) {
        assert p_ptr >= 0;
        assert p_ptr < m_size;

        byte value;

        try {
            m_file.seek(p_ptr);
            value = (byte) m_file.read();
        } catch (final IOException e) {
            throw new MemoryRuntimeException("reading failed " + e);
        }

        return value;
    }

    @Override
    public short readShort(final long p_ptr) {
        assert p_ptr >= 0;
        assert p_ptr + 1 < m_size;

        short value;

        try {
            m_file.seek(p_ptr);
            value = m_file.readShort();
        } catch (final IOException e) {
            throw new MemoryRuntimeException("reading failed " + e);
        }

        return value;
    }

    @Override
    public int readInt(final long p_ptr) {
        assert p_ptr >= 0;
        assert p_ptr + 3 < m_size;

        int value;

        try {
            m_file.seek(p_ptr);
            value = m_file.readInt();
        } catch (final IOException e) {
            throw new MemoryRuntimeException("reading failed " + e);
        }

        return value;
    }

    @Override
    public long readLong(final long p_ptr) {
        assert p_ptr >= 0;
        assert p_ptr + 7 < m_size;

        long value;

        try {
            m_file.seek(p_ptr);
            value = m_file.readLong();
        } catch (final IOException e) {
            throw new MemoryRuntimeException("reading failed " + e);
        }

        return value;
    }

    @Override
    public int writeBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) {
        assert p_ptr >= 0;
        assert p_ptr + p_array.length <= m_size;

        int bytesWritten;

        try {
            m_file.seek(p_ptr);
            m_file.write(p_array, p_arrayOffset, p_length);
            bytesWritten = p_array.length;
        } catch (final IOException e) {
            throw new MemoryRuntimeException("writing failed " + e);
        }

        return bytesWritten;
    }

    @Override
    public void writeByte(final long p_ptr, final byte p_value) {
        assert p_ptr >= 0;
        assert p_ptr < m_size;

        try {
            m_file.seek(p_ptr);
            m_file.writeByte(p_value);
        } catch (final IOException e) {
            throw new MemoryRuntimeException("writing failed " + e);
        }
    }

    @Override
    public void writeShort(final long p_ptr, final short p_value) {
        assert p_ptr > 0;
        assert p_ptr + 1 < m_size;

        try {
            m_file.seek(p_ptr);
            m_file.writeShort(p_value);
        } catch (final IOException e) {
            throw new MemoryRuntimeException("writing failed " + e);
        }
    }

    @Override
    public void writeInt(final long p_ptr, final int p_value) {
        assert p_ptr >= 0;
        assert p_ptr + 3 < m_size;

        try {
            m_file.seek(p_ptr);
            m_file.writeInt(p_value);
        } catch (final IOException e) {
            throw new MemoryRuntimeException("writing failed " + e);
        }
    }

    @Override
    public void writeLong(final long p_ptr, final long p_value) {
        assert p_ptr >= 0;
        assert p_ptr + 7 < m_size;

        try {
            m_file.seek(p_ptr);
            m_file.writeLong(p_value);
        } catch (final IOException e) {
            throw new MemoryRuntimeException("writing failed " + e);
        }
    }

    @Override
    public long readVal(final long p_ptr, final int p_count) {
        assert p_ptr >= 0;
        assert p_ptr + p_count <= m_size;

        long val = 0;

        try {
            m_file.seek(p_ptr);

            for (int i = 0; i < p_count; i++) {
                // input little endian byte order
                // work around not having unsigned data types and "wipe"
                // the sign by & 0xFF
                val |= (long) (m_file.readByte() & 0xFF) << 8 * i;
            }
        } catch (final IOException e) {
            throw new MemoryRuntimeException("reading failed " + e);
        }

        return val;
    }

    @Override
    public void writeVal(final long p_ptr, final long p_val, final int p_count) {
        assert p_ptr >= 0;
        assert p_ptr + p_count <= m_size;

        try {
            m_file.seek(p_ptr);

            for (int i = 0; i < p_count; i++) {
                // output little endian byte order
                m_file.writeByte((int) (p_val >> 8 * i & 0xFF));
            }
        } catch (final IOException e) {
            throw new MemoryRuntimeException("writing failed " + e);
        }
    }
}
