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

/**
 * Interface for an instance which can export/serialize
 * Objects. This instance can (for example) write the contents
 * of an object to a file.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.12.15
 */
public interface Exporter {
    /**
     * Export the provided exportable object to this target.
     * Depending on the implementation this calls at least
     * the exportObject method of the exportable object.
     * But it's possible to trigger some pre- and post processing
     * of data/buffers.
     *
     * @param p_object
     *         Exportable object to serialize/write to this target.
     */
    void exportObject(final Exportable p_object);

    // ----------------------------------------------------------------------

    /**
     * Write a single boolean to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_v
     *         Boolean to write.
     */
    void writeBoolean(final boolean p_v);

    /**
     * Write a single byte to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_v
     *         Byte to write.
     */
    void writeByte(final byte p_v);

    /**
     * Write a short to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_v
     *         Value to write.
     */
    void writeShort(final short p_v);

    /**
     * Write an int to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_v
     *         Value to write.
     */
    void writeInt(final int p_v);

    /**
     * Write a long to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_v
     *         Value to write.
     */
    void writeLong(final long p_v);

    /**
     * Write a float to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_v
     *         Value to write.
     */
    void writeFloat(final float p_v);

    /**
     * Write a double to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_v
     *         Value to write.
     */
    void writeDouble(final double p_v);

    /**
     * Write a compact number to the target. A compact number might be 1, 2, 3 or 4 bytes.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_v
     *         Value to write.
     */
    void writeCompactNumber(int p_v);

    /**
     * Write a string to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_str
     *         String to write
     */
    void writeString(final String p_str);

    /**
     * Write a byte array to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_array
     *         Array to write.
     * @return Number of written elements.
     */
    int writeBytes(final byte[] p_array);

    /**
     * Write a short array to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_array
     *         Array to write.
     * @return Number of written elements.
     */
    int writeShorts(final short[] p_array);

    /**
     * Write an int array to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_array
     *         Array to write.
     * @return Number of written elements.
     */
    int writeInts(final int[] p_array);

    /**
     * Write a long array to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_array
     *         Array to write.
     * @return Number of written elements.
     */
    int writeLongs(final long[] p_array);

    /**
     * Write a byte array to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_array
     *         Array to write.
     * @param p_offset
     *         Offset to start writing from.
     * @param p_length
     *         Number of elements to write.
     * @return Number of written elements.
     */
    int writeBytes(final byte[] p_array, final int p_offset, final int p_length);

    /**
     * Write a short array to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_array
     *         Array to write.
     * @param p_offset
     *         Offset to start writing from.
     * @param p_length
     *         Number of elements to write.
     * @return Number of written elements.
     */
    int writeShorts(final short[] p_array, final int p_offset, final int p_length);

    /**
     * Write an int array to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_array
     *         Array to write.
     * @param p_offset
     *         Offset to start writing from.
     * @param p_length
     *         Number of elements to write.
     * @return Number of written elements.
     */
    int writeInts(final int[] p_array, final int p_offset, final int p_length);

    /**
     * Write a long array to the target.
     * Use this call in your exportable object in the
     * export call to write data to the target.
     *
     * @param p_array
     *         Array to write.
     * @param p_offset
     *         Offset to start writing from.
     * @param p_length
     *         Number of elements to write.
     * @return Number of written elements.
     */
    int writeLongs(final long[] p_array, final int p_offset, final int p_length);

    /**
     * Write a byte array to the target.
     * The array must be written as a self containing object,
     * i.e. with length information
     *
     * @param p_array
     *         Array to write
     */
    void writeByteArray(final byte[] p_array);

    /**
     * Write a short array to the target.
     * The array must be written as a self containing object,
     * i.e. with length information
     *
     * @param p_array
     *         Array to write
     */
    void writeShortArray(final short[] p_array);

    /**
     * Write an int array to the target.
     * The array must be written as a self containing object,
     * i.e. with length information
     *
     * @param p_array
     *         Array to write
     */
    void writeIntArray(final int[] p_array);

    /**
     * Write a long array to the target.
     * The array must be written as a self containing object,
     * i.e. with length information
     *
     * @param p_array
     *         Array to write
     */
    void writeLongArray(final long[] p_array);
}
