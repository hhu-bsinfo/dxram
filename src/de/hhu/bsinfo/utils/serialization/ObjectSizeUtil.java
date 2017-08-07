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
 * Utility class for implementing the sizeofObject call of the ObjectSize interface
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.01.17
 */
public final class ObjectSizeUtil {

    /**
     * Utility class
     */
    private ObjectSizeUtil() {

    }

    /**
     * Get the size of a single compacted number
     *
     * @return Serialization size of a single compacted number
     */
    public static int sizeofCompactedNumber(final int p_number) {
        return CompactNumber.getSizeOfNumber(p_number);
    }

    /**
     * Get the size of a single boolean value (typical size for an implementation)
     *
     * @return Serialization size of a single boolean value
     */
    public static int sizeofBoolean() {
        return Byte.BYTES;
    }

    /**
     * Get the serialization size for a full byte array (including length field)
     *
     * @param p_arr
     *         Array to get the full serialization size for
     * @return Serialization size
     */
    public static int sizeofByteArray(final byte[] p_arr) {
        return CompactNumber.getSizeOfNumber(p_arr.length) + p_arr.length * Byte.BYTES;
    }

    /**
     * Get the serialization size for a full short array (including length field)
     *
     * @param p_arr
     *         Array to get the full serialization size for
     * @return Serialization size
     */
    public static int sizeofShortArray(final short[] p_arr) {
        return CompactNumber.getSizeOfNumber(p_arr.length) + p_arr.length * Short.BYTES;
    }

    /**
     * Get the serialization size for a full int array (including length field)
     *
     * @param p_arr
     *         Array to get the full serialization size for
     * @return Serialization size
     */
    public static int sizeofIntArray(final int[] p_arr) {
        return CompactNumber.getSizeOfNumber(p_arr.length) + p_arr.length * Integer.BYTES;
    }

    /**
     * Get the serialization size for a full long array (including length field)
     *
     * @param p_arr
     *         Array to get the full serialization size for
     * @return Serialization size
     */
    public static int sizeofLongArray(final long[] p_arr) {
        return CompactNumber.getSizeOfNumber(p_arr.length) + p_arr.length * Long.BYTES;
    }

    /**
     * Get the serialization size for a full string (including length field)
     *
     * @param p_str
     *         String to get the full serialization size for
     * @return Serialization size
     */
    public static int sizeofString(final String p_str) {
        return sizeofByteArray(p_str.getBytes());
    }
}
