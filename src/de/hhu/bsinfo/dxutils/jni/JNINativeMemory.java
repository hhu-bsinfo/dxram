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

package de.hhu.bsinfo.dxutils.jni;

/**
 * Replacement for Java's undocumented Unsafe class
 * (this supports proper memcpy'ing of byte arrays).
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 18.02.2016
 */
public final class JNINativeMemory {

    /**
     * Static class, private constuctor.
     */
    private JNINativeMemory() {
    }

    /**
     * Allocate memory on the native operating system's heap.
     *
     * @param p_size
     *     Number of bytes to allocate.
     * @return Pointer/Handle of the memory location with the allocated space.
     */
    public static native long alloc(long p_size);

    /**
     * Free a previously allocated memory block.
     *
     * @param p_addr
     *     Address of the memory block.
     */
    public static native void free(long p_addr);

    /**
     * Dump the contents of the native memory to a file (mainly used for debugging).
     *
     * @param p_addr
     *     Startaddress within our user space memory.
     * @param p_length
     *     Number of bytes to dump.
     * @param p_path
     *     Path to the file to dump to (will be overwritten/created).
     */
    public static native void dump(long p_addr, long p_length, String p_path);

    /**
     * Memset equivalent.
     *
     * @param p_addr
     *     Address.
     * @param p_value
     *     Value to set.
     * @param p_size
     *     Number of bytes to set to the value specified.
     */
    public static native void set(long p_addr, byte p_value, long p_size);

    /**
     * Memcpy equivalent.
     *
     * @param p_addrDest
     *     Destination address to copy to.
     * @param p_addrSrc
     *     Source address to read from.
     * @param p_size
     *     Number of bytes to copy.
     */
    public static native void copy(long p_addrDest, long p_addrSrc, long p_size);

    /**
     * Read from main memory.
     *
     * @param p_addr
     *     Address to start reading from.
     * @param p_array
     *     Target array to read data into.
     * @param p_arrayOffset
     *     Startoffset in array.
     * @param p_length
     *     Number of bytes to read.
     */
    public static native void read(long p_addr, byte[] p_array, int p_arrayOffset, int p_length);

    /**
     * Read from main memory.
     *
     * @param p_addr
     *     Address to start reading from.
     * @param p_array
     *     Target array to read data into.
     * @param p_arrayOffset
     *     Startoffset in array.
     * @param p_length
     *     Number of shorts to read.
     */
    public static native void readShorts(long p_addr, short[] p_array, int p_arrayOffset, int p_length);

    /**
     * Read from main memory.
     *
     * @param p_addr
     *     Address to start reading from.
     * @param p_array
     *     Target array to read data into.
     * @param p_arrayOffset
     *     Startoffset in array.
     * @param p_length
     *     Number of ints to read.
     */
    public static native void readInts(long p_addr, int[] p_array, int p_arrayOffset, int p_length);

    /**
     * Read from main memory.
     *
     * @param p_addr
     *     Address to start reading from.
     * @param p_array
     *     Target array to read data into.
     * @param p_arrayOffset
     *     Startoffset in array.
     * @param p_length
     *     Number of longs to read.
     */
    public static native void readLongs(long p_addr, long[] p_array, int p_arrayOffset, int p_length);

    /**
     * Write data from a byte array to main memory.
     *
     * @param p_addr
     *     Target startaddress to write to.
     * @param p_array
     *     Array with data to write.
     * @param p_arrayOffset
     *     Startoffset within array.
     * @param p_length
     *     Number of bytes to write.
     */
    public static native void write(long p_addr, byte[] p_array, int p_arrayOffset, int p_length);

    /**
     * Write data from a short array to main memory.
     *
     * @param p_addr
     *     Target startaddress to write to.
     * @param p_array
     *     Array with data to write.
     * @param p_arrayOffset
     *     Startoffset within array.
     * @param p_length
     *     Number of shorts to write.
     */
    public static native void writeShorts(long p_addr, short[] p_array, int p_arrayOffset, int p_length);

    /**
     * Write data from an int array to main memory.
     *
     * @param p_addr
     *     Target startaddress to write to.
     * @param p_array
     *     Array with data to write.
     * @param p_arrayOffset
     *     Startoffset within array.
     * @param p_length
     *     Number of ints to write.
     */
    public static native void writeInts(long p_addr, int[] p_array, int p_arrayOffset, int p_length);

    /**
     * Write data from a long array to main memory.
     *
     * @param p_addr
     *     Target startaddress to write to.
     * @param p_array
     *     Array with data to write.
     * @param p_arrayOffset
     *     Startoffset within array.
     * @param p_length
     *     Number of longs to write.
     */
    public static native void writeLongs(long p_addr, long[] p_array, int p_arrayOffset, int p_length);

    /**
     * Read a byte from memory.
     *
     * @param p_addr
     *     Address to read from.
     * @return Read value.
     */
    public static native byte readByte(long p_addr);

    /**
     * Read a short from memory.
     *
     * @param p_addr
     *     Address to read from.
     * @return Read value.
     */
    public static native short readShort(long p_addr);

    /**
     * Read an int from memory.
     *
     * @param p_addr
     *     Address to read from.
     * @return Read value.
     */
    public static native int readInt(long p_addr);

    /**
     * Read a long from memory.
     *
     * @param p_addr
     *     Address to read from.
     * @return Read value.
     */
    public static native long readLong(long p_addr);

    /**
     * Read a value with arbitrary length from memory (max. 8 bytes).
     *
     * @param p_addr
     *     Address to read from.
     * @param p_byteCount
     *     Number of bytes to read into a long.
     * @return Long value with number of bytes read from memory.
     */
    public static native long readValue(long p_addr, int p_byteCount);

    /**
     * Write a byte to memory.
     *
     * @param p_addr
     *     Address to write to.
     * @param p_value
     *     Value to write.
     */
    public static native void writeByte(long p_addr, byte p_value);

    /**
     * Write a short to memory.
     *
     * @param p_addr
     *     Address to write to.
     * @param p_value
     *     Value to write.
     */
    public static native void writeShort(long p_addr, short p_value);

    /**
     * Write an int to memory.
     *
     * @param p_addr
     *     Address to write to.
     * @param p_value
     *     Value to write.
     */
    public static native void writeInt(long p_addr, int p_value);

    /**
     * Write a long to memory.
     *
     * @param p_addr
     *     Address to write to.
     * @param p_value
     *     Value to write.
     */
    public static native void writeLong(long p_addr, long p_value);

    /**
     * Write a value with arbitrary length to memory (max. 8 bytes).
     *
     * @param p_addr
     *     Address to write to.
     * @param p_value
     *     Value to write.
     * @param p_byteCount
     *     of bytes to write.
     */
    public static native void writeValue(long p_addr, long p_value, int p_byteCount);
}
