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

public final class CompactNumber {

    /**
     * Static class.
     */
    private CompactNumber() {

    }

    /**
     * Returns the size of given value as a compacted number
     *
     * @param p_number
     *         the number to compact
     * @return the number of bytes needed for given number
     */
    static int getSizeOfNumber(final int p_number) {
        if (p_number <= 0x7F) {
            return 1;
        } else if (p_number <= 0x3FFF) {
            return 2;
        } else if (p_number <= 0x1FFFFF) {
            return 3;
        } else if (p_number <= 0xFFFFFFF) {
            return 4;
        } else {
            throw new RuntimeException("Number to large to compact. Maximum is " + Math.pow(2, 28));
        }
    }

    /**
     * Compacts given value
     *
     * @param p_number
     *         the number to compact
     * @return the byte array with number in it
     */
    public static byte[] compact(final int p_number) {
        byte[] ret;

        int length = getSizeOfNumber(p_number);
        ret = new byte[length];

        int i;
        for (i = 0; i < length - 1; i++) {
            ret[i] = (byte) ((byte) (p_number >> 7 * i) & 0x7F | 0x80);
        }
        ret[i] = (byte) ((byte) (p_number >> 7 * i) & 0x7F);

        return ret;
    }

    /**
     * Converts given array to an integer
     *
     * @param p_array
     *         the array containing the number
     * @param p_offset
     *         the offset within the array
     * @param p_length
     *         the length of the compacted number
     * @return the number as an integer
     */
    public static int decompact(final byte[] p_array, final int p_offset, final int p_length) {
        int ret = 0;
        for (int i = p_offset; i < p_offset + p_length + 1; i++) {
            ret += (p_array[i] & 0x7F) << i * 7;
        }
        return ret;
    }

}
