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

package de.hhu.bsinfo.utils;

/**
 * Access and (manually) manage memory on the non-jvm heap (unsafe)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.07.2017
 */
public final class UnsafeMemory {
    private static final UnsafeHandler ms_unsafeHandler = UnsafeHandler.getInstance();

    /**
     * Static class
     */
    private UnsafeMemory() {

    }

    /**
     * Allocate memory on the non jvm-heap
     *
     * @param p_size
     *         Size of memory area to allocate
     * @return Start address of the allocated memory area
     */
    public static long allocate(final long p_size) {
        try {
            return ms_unsafeHandler.getUnsafe().allocateMemory(p_size);
        } catch (final Throwable ignored) {
            throw new RuntimeException("Could not allocate unsafe memory (size: " + p_size + ')');
        }
    }

    /**
     * Free a previously allocated memory region
     *
     * @param p_address
     *         Address of memory to free
     */
    public static void free(final long p_address) {
        try {
            ms_unsafeHandler.getUnsafe().freeMemory(p_address);
        } catch (final Throwable ignored) {
            throw new RuntimeException("Could not free unsafe memory, address 0x" + Long.toHexString(p_address));
        }
    }

    /**
     * Set a range of memory to a specified value.
     *
     * @param p_ptr
     *         Pointer to the start location.
     * @param p_size
     *         Number of bytes of the range.
     * @param p_value
     *         Value to set for specified range.
     */
    public static void set(final long p_ptr, final long p_size, final byte p_value) {
        ms_unsafeHandler.getUnsafe().setMemory(p_ptr, p_size, p_value);
    }

    /**
     * Read data from memory into a byte array.
     *
     * @param p_ptr
     *         Start position in memory.
     * @param p_array
     *         Array to read the data into.
     * @param p_arrayOffset
     *         Start offset in array to start writing the bytes to.
     * @param p_length
     *         Number of bytes to read from specified start.
     * @return Number of read elements.
     */
    public static int readBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) {
        ms_unsafeHandler.getUnsafe().copyMemory(null, p_ptr, p_array, UnsafeHandler.getArrayByteOffset() + p_arrayOffset, p_length);

        return p_length;
    }

    /**
     * Read data from memory into a short array.
     *
     * @param p_ptr
     *         Start position in memory.
     * @param p_array
     *         Array to read the data into.
     * @param p_arrayOffset
     *         Start offset in array to start writing the shorts to.
     * @param p_length
     *         Number of shorts to read from specified start.
     * @return Number of read elements.
     */
    public static int readShorts(final long p_ptr, final short[] p_array, final int p_arrayOffset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            p_array[i + p_arrayOffset] = ms_unsafeHandler.getUnsafe().getShort(p_ptr + i * Short.BYTES);
        }

        return p_length;
    }

    /**
     * Read data from memory into an int array.
     *
     * @param p_ptr
     *         Start position in memory.
     * @param p_array
     *         Array to read the data into.
     * @param p_arrayOffset
     *         Start offset in array to start writing the ints to.
     * @param p_length
     *         Number of ints to read from specified start.
     * @return Number of read elements.
     */
    public static int readInts(final long p_ptr, final int[] p_array, final int p_arrayOffset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            p_array[i + p_arrayOffset] = ms_unsafeHandler.getUnsafe().getInt(p_ptr + i * Integer.BYTES);
        }

        return p_length;
    }

    /**
     * Read data from memory into a long array.
     *
     * @param p_ptr
     *         Start position in memory.
     * @param p_array
     *         Array to read the data into.
     * @param p_arrayOffset
     *         Start offset in array to start writing the longs to.
     * @param p_length
     *         Number of longs to read from specified start.
     * @return Number of read elements.
     */
    public static int readLongs(final long p_ptr, final long[] p_array, final int p_arrayOffset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            p_array[i + p_arrayOffset] = ms_unsafeHandler.getUnsafe().getLong(p_ptr + i * Long.BYTES);
        }

        return p_length;
    }

    /**
     * Read data from memory into a float array.
     *
     * @param p_ptr
     *         Start position in memory.
     * @param p_array
     *         Array to read the data into.
     * @param p_arrayOffset
     *         Start offset in array to start writing the floats to.
     * @param p_length
     *         Number of floats to read from specified start.
     * @return Number of read elements.
     */
    public static int readFloats(final long p_ptr, final float[] p_array, final int p_arrayOffset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            p_array[i + p_arrayOffset] = ms_unsafeHandler.getUnsafe().getFloat(p_ptr + i * Float.BYTES);
        }

        return p_length;
    }

    /**
     * Read data from memory into a double array.
     *
     * @param p_ptr
     *         Start position in memory.
     * @param p_array
     *         Array to read the data into.
     * @param p_arrayOffset
     *         Start offset in array to start writing the doubles to.
     * @param p_length
     *         Number of doubles to read from specified start.
     * @return Number of read elements.
     */
    public static int readDoubles(final long p_ptr, final double[] p_array, final int p_arrayOffset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            p_array[i + p_arrayOffset] = ms_unsafeHandler.getUnsafe().getDouble(p_ptr + i * Double.BYTES);
        }

        return p_length;
    }

    /**
     * Read a single byte value.
     *
     * @param p_ptr
     *         Memory position to read from.
     * @return Byte read.
     */
    public static byte readByte(final long p_ptr) {
        return ms_unsafeHandler.getUnsafe().getByte(p_ptr);
    }

    /**
     * Read a single short value.
     *
     * @param p_ptr
     *         Memory position to read from.
     * @return Short read.
     */
    public static short readShort(final long p_ptr) {
        return ms_unsafeHandler.getUnsafe().getShort(p_ptr);
    }

    /**
     * Read a single int value.
     *
     * @param p_ptr
     *         Memory position to read from.
     * @return Int read.
     */
    public static int readInt(final long p_ptr) {
        return ms_unsafeHandler.getUnsafe().getInt(p_ptr);
    }

    /**
     * Read a single long value.
     *
     * @param p_ptr
     *         Memory position to read from.
     * @return Long read.
     */
    public static long readLong(final long p_ptr) {
        return ms_unsafeHandler.getUnsafe().getLong(p_ptr);
    }

    /**
     * Read a single float value.
     *
     * @param p_ptr
     *         Memory position to read from.
     * @return Float read.
     */
    public static float readFloat(final long p_ptr) {
        return ms_unsafeHandler.getUnsafe().getFloat(p_ptr);
    }

    /**
     * Read a single double value.
     *
     * @param p_ptr
     *         Memory position to read from.
     * @return Double read.
     */
    public static double readDouble(final long p_ptr) {
        return ms_unsafeHandler.getUnsafe().getDouble(p_ptr);
    }

    /**
     * Write an array of bytes to memory.
     *
     * @param p_ptr
     *         Start address to write to.
     * @param p_array
     *         Array with data to write.
     * @param p_arrayOffset
     *         Offset in array to start reading the data from.
     * @param p_length
     *         Number of elements to write.
     * @return Number of written elements
     */
    public static int writeBytes(final long p_ptr, final byte[] p_array, final int p_arrayOffset, final int p_length) {
        ms_unsafeHandler.getUnsafe().copyMemory(p_array, UnsafeHandler.getArrayByteOffset() + p_arrayOffset, null, p_ptr, p_length);

        return p_length;
    }

    /**
     * Write an array of shorts to memory.
     *
     * @param p_ptr
     *         Start address to write to.
     * @param p_array
     *         Array with data to write.
     * @param p_arrayOffset
     *         Offset in array to start reading the data from.
     * @param p_length
     *         Number of elements to write.
     * @return Number of written elements
     */
    public static int writeShorts(final long p_ptr, final short[] p_array, final int p_arrayOffset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            ms_unsafeHandler.getUnsafe().putShort(p_ptr + i * Short.BYTES, p_array[i + p_arrayOffset]);
        }

        return p_length;
    }

    /**
     * Write an array of ints to memory.
     *
     * @param p_ptr
     *         Start address to write to.
     * @param p_array
     *         Array with data to write.
     * @param p_arrayOffset
     *         Offset in array to start reading the data from.
     * @param p_length
     *         Number of elements to write.
     * @return Number of written elements
     */
    public static int writeInts(final long p_ptr, final int[] p_array, final int p_arrayOffset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            ms_unsafeHandler.getUnsafe().putInt(p_ptr + i * Integer.BYTES, p_array[i + p_arrayOffset]);
        }

        return p_length;
    }

    /**
     * Write an array of longs to memory.
     *
     * @param p_ptr
     *         Start address to write to.
     * @param p_array
     *         Array with data to write.
     * @param p_arrayOffset
     *         Offset in array to start reading the data from.
     * @param p_length
     *         Number of elements to write.
     * @return Number of written elements
     */
    public static int writeLongs(final long p_ptr, final long[] p_array, final int p_arrayOffset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            ms_unsafeHandler.getUnsafe().putLong(p_ptr + i * Long.BYTES, p_array[i + p_arrayOffset]);
        }

        return p_length;
    }

    /**
     * Write an array of floats to memory.
     *
     * @param p_ptr
     *         Start address to write to.
     * @param p_array
     *         Array with data to write.
     * @param p_arrayOffset
     *         Offset in array to start reading the data from.
     * @param p_length
     *         Number of elements to write.
     * @return Number of written elements
     */
    public static int writeFloats(final long p_ptr, final float[] p_array, final int p_arrayOffset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            ms_unsafeHandler.getUnsafe().putFloat(p_ptr + i * Float.BYTES, p_array[i + p_arrayOffset]);
        }

        return p_length;
    }

    /**
     * Write an array of doubles to memory.
     *
     * @param p_ptr
     *         Start address to write to.
     * @param p_array
     *         Array with data to write.
     * @param p_arrayOffset
     *         Offset in array to start reading the data from.
     * @param p_length
     *         Number of elements to write.
     * @return Number of written elements
     */
    public static int writeDoubles(final long p_ptr, final double[] p_array, final int p_arrayOffset, final int p_length) {
        for (int i = 0; i < p_length; i++) {
            ms_unsafeHandler.getUnsafe().putDouble(p_ptr + i * Double.BYTES, p_array[i + p_arrayOffset]);
        }

        return p_length;
    }

    /**
     * Write a single byte value to memory.
     *
     * @param p_ptr
     *         Address to write to.
     * @param p_value
     *         Value to write.
     */
    public static void writeByte(final long p_ptr, final byte p_value) {
        ms_unsafeHandler.getUnsafe().putByte(p_ptr, p_value);
    }

    /**
     * Write a single short value to memory.
     *
     * @param p_ptr
     *         Address to write to.
     * @param p_value
     *         Value to write.
     */
    public static void writeShort(final long p_ptr, final short p_value) {
        ms_unsafeHandler.getUnsafe().putShort(p_ptr, p_value);
    }

    /**
     * Write a single int value to memory.
     *
     * @param p_ptr
     *         Address to write to.
     * @param p_value
     *         Value to write.
     */
    public static void writeInt(final long p_ptr, final int p_value) {
        ms_unsafeHandler.getUnsafe().putInt(p_ptr, p_value);
    }

    /**
     * Write a single long value to memory.
     *
     * @param p_ptr
     *         Address to write to.
     * @param p_value
     *         Value to write.
     */
    public static void writeLong(final long p_ptr, final long p_value) {
        ms_unsafeHandler.getUnsafe().putLong(p_ptr, p_value);
    }

    /**
     * Write a single float value to memory.
     *
     * @param p_ptr
     *         Address to write to.
     * @param p_value
     *         Value to write.
     */
    public static void writeFloat(final long p_ptr, final float p_value) {
        ms_unsafeHandler.getUnsafe().putFloat(p_ptr, p_value);
    }

    /**
     * Write a single double value to memory.
     *
     * @param p_ptr
     *         Address to write to.
     * @param p_value
     *         Value to write.
     */
    public static void writeDouble(final long p_ptr, final double p_value) {
        ms_unsafeHandler.getUnsafe().putDouble(p_ptr, p_value);
    }
}
