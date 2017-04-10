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
 * Convert normal integer values to multi byte representation. A multi
 * byte is an array of bytes which only requires the length needed by the
 * size of the integer number. The first 7 bits of a byte are used to store
 * a part of the number where the last bit is used to indicate if another byte
 * with more data is next or the current byte is the last one.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public final class DynamicMultiByteUnsignedInteger {
    /**
     * Static class
     */
    private DynamicMultiByteUnsignedInteger() {
    }

    /**
     * Convert a value to a multi byte array representation.
     *
     * @param p_val
     *     Value to convert.
     * @return Allocated byte array with converted value.
     */
    public static byte[] toMultiByte(final byte p_val) {
        return toMultiByte((long) p_val);
    }

    /**
     * Convert a value to a multi byte array representation.
     *
     * @param p_val
     *     Value to convert.
     * @return Allocated byte array with converted value.
     */
    public static byte[] toMultiByte(final short p_val) {
        return toMultiByte((long) p_val);
    }

    /**
     * Convert a value to a multi byte array representation.
     *
     * @param p_val
     *     Value to convert.
     * @return Allocated byte array with converted value.
     */
    public static byte[] toMultiByte(final int p_val) {
        return toMultiByte((long) p_val);
    }

    /**
     * Convert a value to a multi byte array representation.
     *
     * @param p_val
     *     Value to convert.
     * @return Allocated byte array with converted value.
     */
    public static byte[] toMultiByte(final long p_val) {
        byte[] array = new byte[getMultiByteCount(p_val)];

        toMultiByte(p_val, array, 0);

        return array;
    }

    /**
     * Convert a value to a multi byte array representation. The array
     * provided will be used to store the multi byte data. Make sure
     * there is enough space reserved to avoid creating invalid data.
     *
     * @param p_val
     *     Value to convert.
     * @param p_array
     *     Array to store the converted value to.
     * @param p_arrayOffset
     *     Position in array to start writing the value to.
     * @return Size of multi byte representation.
     */
    public static int toMultiByte(final byte p_val, final byte[] p_array, final int p_arrayOffset) {
        return toMultiByte((long) p_val, p_array, p_arrayOffset);
    }

    /**
     * Convert a value to a multi byte array representation. The array
     * provided will be used to store the multi byte data. Make sure
     * there is enough space reserved to avoid creating invalid data.
     *
     * @param p_val
     *     Value to convert.
     * @param p_array
     *     Array to store the converted value to.
     * @param p_arrayOffset
     *     Position in array to start writing the value to.
     * @return Size of multi byte representation.
     */
    public static int toMultiByte(final short p_val, final byte[] p_array, final int p_arrayOffset) {
        return toMultiByte((long) p_val, p_array, p_arrayOffset);
    }

    /**
     * Convert a value to a multi byte array representation. The array
     * provided will be used to store the multi byte data. Make sure
     * there is enough space reserved to avoid creating invalid data.
     *
     * @param p_val
     *     Value to convert.
     * @param p_array
     *     Array to store the converted value to.
     * @param p_arrayOffset
     *     Position in array to start writing the value to.
     * @return Size of multi byte representation.
     */
    public static int toMultiByte(final int p_val, final byte[] p_array, final int p_arrayOffset) {
        return toMultiByte((long) p_val, p_array, p_arrayOffset);
    }

    /**
     * Convert a value to a multi byte array representation. The array
     * provided will be used to store the multi byte data. Make sure
     * there is enough space reserved to avoid creating invalid data.
     *
     * @param p_val
     *     Value to convert.
     * @param p_array
     *     Array to store the converted value to.
     * @param p_arrayOffset
     *     Position in array to start writing the value to.
     * @return Size of multi byte representation.
     */
    public static int toMultiByte(final long p_val, final byte[] p_array, final int p_arrayOffset) {
        int numLength = getMultiByteCount(p_val);

        for (int i = p_arrayOffset; i < p_arrayOffset + numLength; i++) {
            p_array[i] = (byte) (p_val >> 7 * i & 0x7F);
            // set highest bit to indicate further bytes available
            if (i + 1 < p_arrayOffset + numLength) {
                p_array[i] |= 0x80;
            }
        }

        return numLength;
    }

    /**
     * Extract a multi byte value from the specified array.
     *
     * @param p_array
     *     Array to extract the value from.
     * @param p_arrayOffset
     *     Start offset of the multi byte value.
     * @return Extracted multi byte value.
     */
    public static long fromMultiByte(final byte[] p_array, final int p_arrayOffset) {
        int pos = 0;
        long ret = 0;

        while (pos < 9) {
            ret |= (long) (p_array[p_arrayOffset + pos] & 0x7F) << 7 * pos;
            if ((p_array[p_arrayOffset + pos] & 0x80) != 1) {
                break;
            }
        }

        return ret;
    }

    /**
     * Get the size of a value if it is converted into a multi byte value.
     *
     * @param p_val
     *     Value to get the size of.
     * @return Number of bytes needed to store this value as a multi byte value.
     */
    public static int getMultiByteCount(final long p_val) {
        assert p_val >= 0;

        int size = -1;

        // max supported 2^63
        if (p_val < 0) {
            size = -1;
            // 2^7
        } else if (p_val < 0x80) {
            size = 1;
            // 2^14
        } else if (p_val < 0x4000) {
            size = 2;
            // 2^21
        } else if (p_val < 0x200000) {
            size = 3;
            // 2^28
        } else if (p_val < 0x10000000) {
            size = 4;
            // 2^35
        } else if (p_val < 0x800000000L) {
            size = 5;
            // 2^42
        } else if (p_val < 0x40000000000L) {
            size = 6;
            // 2^49
        } else if (p_val < 0x2000000000000L) {
            size = 7;
            // 2^56
        } else if (p_val < 0x100000000000000L) {
            size = 8;
            // 2^63
        } else {
            size = 9;
        }

        return size;
    }
}
