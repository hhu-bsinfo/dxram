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

package de.hhu.bsinfo.utils;

/**
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.03.2017
 */
public final class JNINativeCRCGenerator {

    /**
     * Static class, private constuctor.
     */
    private JNINativeCRCGenerator() {
    }

    /**
     * Update CRC32 checksum.
     *
     * @param p_checksum
     *     the current checksum, 0 if all data is given at once.
     * @param p_data
     *     the byte array with data to hash.
     * @param p_offset
     *     the offset within given array.
     * @param p_length
     *     the length of the data to hash.
     * @return the CRC32C checksum.
     */
    public static native int hash(final int p_checksum, final byte[] p_data, final int p_offset, final int p_length);
}
