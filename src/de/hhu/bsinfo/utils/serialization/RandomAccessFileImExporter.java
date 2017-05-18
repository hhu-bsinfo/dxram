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
        try {
            m_file.writeInt(p_array.length);
            writeBytes(p_array);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeShortArray(final short[] p_array) {
        try {
            m_file.writeInt(p_array.length);
            writeShorts(p_array);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeIntArray(final int[] p_array) {
        try {
            m_file.writeInt(p_array.length);
            writeInts(p_array);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeLongArray(final long[] p_array) {
        try {
            m_file.writeInt(p_array.length);
            writeLongs(p_array);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void writeStringArray(final String[] p_array) {
        try {
            m_file.writeInt(p_array.length);

            for (int i = 0; i < p_array.length; i++) {
                writeString(p_array[i]);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void importObject(final Importable p_object) {
        p_object.importObject(this);
    }

    @Override
    public boolean readBoolean() {
        try {
            return m_file.readBoolean();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte readByte() {
        try {
            return m_file.readByte();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public short readShort() {
        try {
            return m_file.readShort();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readInt() {
        try {
            return m_file.readInt();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long readLong() {
        try {
            return m_file.readLong();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public float readFloat() {
        try {
            return m_file.readFloat();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public double readDouble() {
        try {
            return m_file.readDouble();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String readString() {
        return new String(readByteArray());
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
    public byte[] readByteArray() {
        try {
            byte[] arr = new byte[m_file.readInt()];
            readBytes(arr);
            return arr;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public short[] readShortArray() {
        try {
            short[] arr = new short[m_file.readInt()];
            readShorts(arr);
            return arr;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int[] readIntArray() {
        try {
            int[] arr = new int[m_file.readInt()];
            readInts(arr);
            return arr;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long[] readLongArray() {
        try {
            long[] arr = new long[m_file.readInt()];
            readLongs(arr);
            return arr;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String[] readStringArray() {
        try {
            String[] arr = new String[m_file.readInt()];

            for (int i = 0; i < arr.length; i++) {
                arr[i] = readString();
            }

            return arr;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
