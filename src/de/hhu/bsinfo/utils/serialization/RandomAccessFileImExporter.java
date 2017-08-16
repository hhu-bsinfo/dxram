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

package de.hhu.bsinfo.utils.serialization;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Importer/Exporter for a RandomAccessFile
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 30.01.2017
 */
public class RandomAccessFileImExporter implements Importer, Exporter {
    private RandomAccessFile m_file;

    /**
     * Constructor
     *
     * @param p_fileName
     *         Name of the file to open
     * @throws FileNotFoundException
     *         If file does not exist
     */
    public RandomAccessFileImExporter(final String p_fileName) throws FileNotFoundException {
        m_file = new RandomAccessFile(p_fileName, "rw");
    }

    /**
     * Constructor
     *
     * @param p_file
     *         File to open
     * @throws FileNotFoundException
     *         If file does not exist
     */
    public RandomAccessFileImExporter(final File p_file) throws FileNotFoundException {
        m_file = new RandomAccessFile(p_file, "rw");
    }

    /**
     * Close the backend flushing all buffers
     */
    public void close() {
        try {
            m_file.close();
        } catch (final IOException ignored) {

        }
    }

    @Override
    public void exportObject(final Exportable p_object) {
        p_object.exportObject(this);
    }

    @Override
    public void writeBoolean(final boolean p_v) {
        try {
            m_file.writeBoolean(p_v);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeByte(final byte p_v) {
        try {
            m_file.writeByte(p_v);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeShort(final short p_v) {
        try {
            m_file.writeShort(p_v);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeInt(final int p_v) {
        try {
            m_file.writeInt(p_v);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeLong(final long p_v) {
        try {
            m_file.writeLong(p_v);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeFloat(final float p_v) {
        try {
            m_file.writeFloat(p_v);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeDouble(final double p_v) {
        try {
            m_file.writeDouble(p_v);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeCompactNumber(int p_v) {
        byte[] number = CompactNumber.compact(p_v);
        writeBytes(number);
    }

    @Override
    public void writeString(final String p_str) {
        writeByteArray(p_str.getBytes());
    }

    @Override
    public int writeBytes(final byte[] p_array) {
        try {
            m_file.write(p_array);
            return p_array.length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int writeShorts(final short[] p_array) {
        try {
            for (int i = 0; i < p_array.length; i++) {
                m_file.writeShort(p_array[i]);
            }
            return p_array.length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int writeInts(final int[] p_array) {
        try {
            for (int i = 0; i < p_array.length; i++) {
                m_file.writeInt(p_array[i]);
            }
            return p_array.length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int writeLongs(final long[] p_array) {
        try {
            for (int i = 0; i < p_array.length; i++) {
                m_file.writeLong(p_array[i]);
            }
            return p_array.length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int writeBytes(final byte[] p_array, final int p_offset, final int p_length) {
        try {
            m_file.write(p_array, p_offset, p_length);
            return p_length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int writeShorts(final short[] p_array, final int p_offset, final int p_length) {
        try {
            for (int i = p_offset; i < p_length; i++) {
                m_file.writeShort(p_array[i]);
            }
            return p_length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int writeInts(final int[] p_array, final int p_offset, final int p_length) {
        try {
            for (int i = p_offset; i < p_length; i++) {
                m_file.writeInt(p_array[i]);
            }
            return p_length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int writeLongs(final long[] p_array, final int p_offset, final int p_length) {
        try {
            for (int i = p_offset; i < p_length; i++) {
                m_file.writeLong(p_array[i]);
            }
            return p_length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
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
    public void importObject(final Importable p_object) {
        p_object.importObject(this);
    }

    @Override
    public boolean readBoolean(final boolean p_bool) {
        try {
            return m_file.readBoolean();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte readByte(final byte p_byte) {
        try {
            return m_file.readByte();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public short readShort(final short p_short) {
        try {
            return m_file.readShort();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readInt(final int p_int) {
        try {
            return m_file.readInt();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long readLong(final long p_long) {
        try {
            return m_file.readLong();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public float readFloat(final float p_float) {
        try {
            return m_file.readFloat();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public double readDouble(final double p_double) {
        try {
            return m_file.readDouble();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readCompactNumber(int p_int) {
        byte[] tmp = new byte[4];
        int i;
        for (i = 0; i < Integer.BYTES; i++) {
            tmp[i] = readByte((byte) 0);
            if ((tmp[i] & 0x80) == 0) {
                break;
            }
        }

        return CompactNumber.decompact(tmp, 0, i);
    }

    @Override
    public String readString(final String p_string) {
        return new String(readByteArray(null));
    }

    @Override
    public int readBytes(final byte[] p_array) {
        try {
            return m_file.read(p_array);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readShorts(final short[] p_array) {
        try {
            for (int i = 0; i < p_array.length; i++) {
                p_array[i] = m_file.readShort();
            }
            return p_array.length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readInts(final int[] p_array) {
        try {
            for (int i = 0; i < p_array.length; i++) {
                p_array[i] = m_file.readInt();
            }
            return p_array.length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readLongs(final long[] p_array) {
        try {
            for (int i = 0; i < p_array.length; i++) {
                p_array[i] = m_file.readLong();
            }
            return p_array.length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readBytes(final byte[] p_array, final int p_offset, final int p_length) {
        try {
            return m_file.read(p_array, p_offset, p_length);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readShorts(final short[] p_array, final int p_offset, final int p_length) {
        try {
            for (int i = p_offset; i < p_length; i++) {
                p_array[i] = m_file.readShort();
            }
            return p_array.length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readInts(final int[] p_array, final int p_offset, final int p_length) {
        try {
            for (int i = p_offset; i < p_length; i++) {
                p_array[i] = m_file.readInt();
            }
            return p_array.length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readLongs(final long[] p_array, final int p_offset, final int p_length) {
        try {
            for (int i = p_offset; i < p_length; i++) {
                p_array[i] = m_file.readLong();
            }
            return p_array.length;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
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
}
