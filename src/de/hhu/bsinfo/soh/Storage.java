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

/**
 * Interface to describe a type of storage/memory to store
 * data to.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.11.2015
 */
public interface Storage {
    /**
     * Allocate/Initialize the storage.
     * Make sure to call this before calling any other methods.
     *
     * @param p_size
     *     Size of the storage in bytes.
     */
    void allocate(long p_size);

    /**
     * Free/Cleanup the storage.
     * Make sure to call this before object destruction.
     */
    void free();

    /**
     * Get the total allocated size of the storage.
     *
     * @return Size of the storage.
     */
    long getSize();

    /**
     * Set a range of memory to a specified value.
     *
     * @param p_ptr
     *     Pointer to the start location.
     * @param p_size
     *     Number of bytes of the range.
     * @param p_value
     *     Value to set for specified range.
     */
    void set(long p_ptr, long p_size, byte p_value);

    /**
     * Read data from the storage into a byte array.
     *
     * @param p_ptr
     *     Start position in storage.
     * @param p_array
     *     Array to read the data into.
     * @param p_arrayOffset
     *     Start offset in array to start writing the bytes to.
     * @param p_length
     *     Number of bytes to read from specified start.
     * @return Number of read elements.
     */
    int readBytes(long p_ptr, byte[] p_array, int p_arrayOffset, int p_length);

    /**
     * Read data from the storage into a short array.
     *
     * @param p_ptr
     *     Start position in storage.
     * @param p_array
     *     Array to read the data into.
     * @param p_arrayOffset
     *     Start offset in array to start writing the shorts to.
     * @param p_length
     *     Number of shorts to read from specified start.
     * @return Number of read elements.
     */
    int readShorts(long p_ptr, short[] p_array, int p_arrayOffset, int p_length);

    /**
     * Read data from the storage into an int array.
     *
     * @param p_ptr
     *     Start position in storage.
     * @param p_array
     *     Array to read the data into.
     * @param p_arrayOffset
     *     Start offset in array to start writing the ints to.
     * @param p_length
     *     Number of ints to read from specified start.
     * @return Number of read elements.
     */
    int readInts(long p_ptr, int[] p_array, int p_arrayOffset, int p_length);

    /**
     * Read data from the storage into a long array.
     *
     * @param p_ptr
     *     Start position in storage.
     * @param p_array
     *     Array to read the data into.
     * @param p_arrayOffset
     *     Start offset in array to start writing the longs to.
     * @param p_length
     *     Number of longs to read from specified start.
     * @return Number of read elements.
     */
    int readLongs(long p_ptr, long[] p_array, int p_arrayOffset, int p_length);

    /**
     * Read a single byte value.
     *
     * @param p_ptr
     *     Position to read from.
     * @return Byte read.
     */
    byte readByte(long p_ptr);

    /**
     * Read a single short value.
     *
     * @param p_ptr
     *     Position to read from.
     * @return Short read.
     */
    short readShort(long p_ptr);

    /**
     * Read a single int value.
     *
     * @param p_ptr
     *     Position to read from.
     * @return Int read.
     */
    int readInt(long p_ptr);

    /**
     * Read a single long value.
     *
     * @param p_ptr
     *     Position to read from.
     * @return Long read.
     */
    long readLong(long p_ptr);

    /**
     * Write an array of bytes to the storage.
     *
     * @param p_ptr
     *     Start address to write to.
     * @param p_array
     *     Array with data to write.
     * @param p_arrayOffset
     *     Offset in array to start reading the data from.
     * @param p_length
     *     Number of elements to write.
     * @return Number of written elements
     */
    int writeBytes(long p_ptr, byte[] p_array, int p_arrayOffset, int p_length);

    /**
     * Write an array of shorts to the storage.
     *
     * @param p_ptr
     *     Start address to write to.
     * @param p_array
     *     Array with data to write.
     * @param p_arrayOffset
     *     Offset in array to start reading the data from.
     * @param p_length
     *     Number of elements to write.
     * @return Number of written elements
     */
    int writeShorts(long p_ptr, short[] p_array, int p_arrayOffset, int p_length);

    /**
     * Write an array of ints to the storage.
     *
     * @param p_ptr
     *     Start address to write to.
     * @param p_array
     *     Array with data to write.
     * @param p_arrayOffset
     *     Offset in array to start reading the data from.
     * @param p_length
     *     Number of elements to write.
     * @return Number of written elements
     */
    int writeInts(long p_ptr, int[] p_array, int p_arrayOffset, int p_length);

    /**
     * Write an array of longs to the storage.
     *
     * @param p_ptr
     *     Start address to write to.
     * @param p_array
     *     Array with data to write.
     * @param p_arrayOffset
     *     Offset in array to start reading the data from.
     * @param p_length
     *     Number of elements to write.
     * @return Number of written elements
     */
    int writeLongs(long p_ptr, long[] p_array, int p_arrayOffset, int p_length);

    /**
     * Write a single byte value to the storage.
     *
     * @param p_ptr
     *     Address to write to.
     * @param p_value
     *     Value to write.
     */
    void writeByte(long p_ptr, byte p_value);

    /**
     * Write a single short value to the storage.
     *
     * @param p_ptr
     *     Address to write to.
     * @param p_value
     *     Value to write.
     */
    void writeShort(long p_ptr, short p_value);

    /**
     * Write a single int value to the storage.
     *
     * @param p_ptr
     *     Address to write to.
     * @param p_value
     *     Value to write.
     */
    void writeInt(long p_ptr, int p_value);

    /**
     * Write a single long value to the storage.
     *
     * @param p_ptr
     *     Address to write to.
     * @param p_value
     *     Value to write.
     */
    void writeLong(long p_ptr, long p_value);

    /**
     * Read a value with specified number of bytes length from the storage.
     *
     * @param p_ptr
     *     Address to read from.
     * @param p_count
     *     Number of bytes the value is stored to.
     * @return Value read.
     */
    long readVal(long p_ptr, int p_count);

    /**
     * Write a value with specified number of bytes length to the storage.
     *
     * @param p_ptr
     *     Address to write to.
     * @param p_val
     *     Value to write.
     * @param p_count
     *     Number of bytes the value should occupy.
     */
    void writeVal(long p_ptr, long p_val, int p_count);
}
