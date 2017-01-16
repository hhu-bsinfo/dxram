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

package de.hhu.bsinfo.utils.serialization;

/**
 * Interface for an instance which can import/de-serialize
 * Objects. This instance can (for example) read the contents
 * of an object from a file.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.12.15
 */
public interface Importer {
    /**
     * TaskScriptImport/read data from the target to the provided already
     * allocated object.
     * Depending on the implementation this calls at least
     * the importObject method of the importable object.
     * But it's possible to trigger some pre- and post processing
     * of data/buffers.
     *
     * @param p_object
     *     Importable Pre-allocated object to read data from the target into.
     */
    void importObject(final Importable p_object);

    // ----------------------------------------------------------------------

    /**
     * Read a single boolean from the target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @return Boolean read.
     */
    boolean readBoolean();

    /**
     * Read a single byte from the target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @return Byte read.
     */
    byte readByte();

    /**
     * Read a short from the target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @return Value read.
     */
    short readShort();

    /**
     * Read an int from the target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @return Value read.
     */
    int readInt();

    /**
     * Read a long from the target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @return Value read.
     */
    long readLong();

    /**
     * Read a float from the target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @return Value read.
     */
    float readFloat();

    /**
     * Read a double from the target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @return Value read.
     */
    double readDouble();

    /**
     * Read a String from the target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @return String read.
     */
    String readString();

    /**
     * Read data into a byte array from target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @param p_array
     *     Array to read into.
     * @return Number of bytes read.
     */
    int readBytes(final byte[] p_array);

    /**
     * Read data into a short array from target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @param p_array
     *     Array to read into.
     * @return Number of shorts read;
     */
    int readShorts(final short[] p_array);

    /**
     * Read data into an int array from target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @param p_array
     *     Array to read into.
     * @return Number of ints read;
     */
    int readInts(final int[] p_array);

    /**
     * Read data into a long array from target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @param p_array
     *     Array to read into.
     * @return Number of longs read;
     */
    int readLongs(final long[] p_array);

    /**
     * Read data into a byte array from target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @param p_array
     *     Array to read into.
     * @param p_offset
     *     Offset to start in the array for reading into.
     * @param p_length
     *     Number of bytes to read.
     * @return Number of bytes read.
     */
    int readBytes(final byte[] p_array, final int p_offset, final int p_length);

    /**
     * Read data into a short array from target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @param p_array
     *     Array to read into.
     * @param p_offset
     *     Offset to start in the array for reading into.
     * @param p_length
     *     Number of shorts to read.
     * @return Number of shorts read.
     */
    int readShorts(final short[] p_array, final int p_offset, final int p_length);

    /**
     * Read data into an int array from target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @param p_array
     *     Array to read into.
     * @param p_offset
     *     Offset to start in the array for reading into.
     * @param p_length
     *     Number of ints to read.
     * @return Number of ints read.
     */
    int readInts(final int[] p_array, final int p_offset, final int p_length);

    /**
     * Read data into a long array from target.
     * Use this call in your importable object in the
     * import call to read data from the target.
     *
     * @param p_array
     *     Array to read into.
     * @param p_offset
     *     Offset to start in the array for reading into.
     * @param p_length
     *     Number of longs to read.
     * @return Number of longs read.
     */
    int readLongs(final long[] p_array, final int p_offset, final int p_length);

    /**
     * Read a byte array from the target.
     * The array to be read must be stored as self containing object,
     * i.e. with length information
     *
     * @return New array with data read.
     */
    byte[] readByteArray();

    /**
     * Read a short array from the target.
     * The array to be read must be stored as self containing object,
     * i.e. with length information
     *
     * @return New array with data read.
     */
    short[] readShortArray();

    /**
     * Read an int array from the target.
     * The array to be read must be stored as self containing object,
     * i.e. with length information
     *
     * @return New array with data read.
     */
    int[] readIntArray();

    /**
     * Read a long array from the target.
     * The array to be read must be stored as self containing object,
     * i.e. with length information
     *
     * @return New array with data read.
     */
    long[] readLongArray();
}
