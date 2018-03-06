/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.lookup.overlay.storage;

/**
 * Skeleton for superpeer metadata
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.10.2016
 */
abstract class AbstractMetadata {

    /**
     * Stores all metadata from given byte array
     *
     * @param p_data
     *     the byte array
     * @param p_offset
     *     the offset within the byte array
     * @param p_size
     *     the number of bytes
     * @return the amount of stored metadata
     */
    public abstract int storeMetadata(byte[] p_data, int p_offset, int p_size);

    /**
     * Returns all metadata
     *
     * @return all metadata in a byte array
     */
    public abstract byte[] receiveAllMetadata();

    /**
     * Returns all entries in area
     *
     * @param p_bound1
     *     the first bound
     * @param p_bound2
     *     the second bound
     * @return corresponding metadata in a byte array
     */
    public abstract byte[] receiveMetadataInRange(short p_bound1, short p_bound2);

    /**
     * Removes all metadata outside of area
     *
     * @param p_bound1
     *     the first bound
     * @param p_bound2
     *     the second bound
     * @return number of removed metadata
     */
    public abstract int removeMetadataOutsideOfRange(short p_bound1, short p_bound2);

    /**
     * Returns the amount of metadata in area
     *
     * @param p_bound1
     *     the first bound
     * @param p_bound2
     *     the second bound
     * @return the amount of metadata
     */
    public abstract int quantifyMetadata(short p_bound1, short p_bound2);
}
