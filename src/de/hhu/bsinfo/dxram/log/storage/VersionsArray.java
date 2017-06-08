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

package de.hhu.bsinfo.dxram.log.storage;

import java.util.Arrays;

/**
 * Array to store versions
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 04.02.2017
 */
class VersionsArray {

    private int[] m_table;
    private int m_count;
    private int m_elementCapacity;

    // Constructors

    /**
     * Creates an instance of VersionsArray
     *
     * @param p_initialElementCapacity
     *     the initial capacity of VersionsArray
     */
    VersionsArray(final int p_initialElementCapacity) {
        super();

        m_count = 0;
        m_elementCapacity = p_initialElementCapacity;
        if (p_initialElementCapacity == 0) {
            m_elementCapacity = 100;
        }

        m_table = new int[m_elementCapacity * 2];
        Arrays.fill(m_table, Version.INVALID_VERSION);
    }

    // Getter / Setter

    /**
     * Returns the number of keys in VersionsArray
     *
     * @return the number of keys in VersionsArray
     */
    final int size() {
        return m_count;
    }

    /**
     * Returns the size of the array
     *
     * @return the capacity
     */
    final int capacity() {
        return m_table.length / 2;
    }

    /**
     * Clears VersionsArray
     */
    final void clear() {
        if (m_count != 0) {
            Arrays.fill(m_table, Version.INVALID_VERSION);
            m_count = 0;
        }
    }

    // Methods

    /**
     * Returns the value to which the specified key is mapped in VersionsArray
     *
     * @param p_key
     *     the searched key (is incremented before insertion to avoid 0)
     * @param p_lowestCID
     *     the lowest CID at the time the versions were read-in
     * @return the value to which the key is mapped in VersionsArray
     */
    final Version get(final long p_key, final long p_lowestCID) {
        int index = (int) ((p_key - p_lowestCID) * 2);
        short epoch = (short) getEpoch(index);

        if (epoch != -1) {
            return new Version(epoch, getVersion(index));
        } else {
            return null;
        }
    }

    /**
     * Maps the given key to the given value in VersionsArray
     *
     * @param p_key
     *     the key (is incremented before insertion to avoid 0)
     * @param p_epoch
     *     the epoch
     * @param p_version
     *     the version
     * @param p_idOffset
     *     the lowest ChunkID in range
     */
    final void put(final long p_key, final int p_epoch, final int p_version, final long p_idOffset) {
        if (set((int) ((p_key - p_idOffset) * 2), p_epoch, p_version)) {
            m_count++;
        }
    }

    /**
     * Returns the value to which the specified key is mapped in VersionsArray
     *
     * @param p_key
     *     the searched key (is incremented before insertion to avoid 0)
     * @param p_idOffset
     *     the lowest ChunkID in range
     * @return the value to which the key is mapped in VersionsArray
     */
    final short getEpoch(final long p_key, final long p_idOffset) {
        int index = (int) ((p_key - p_idOffset) * 2);
        return (short) getEpoch(index);
    }

    /**
     * Returns the value to which the specified key is mapped in VersionsArray
     *
     * @param p_key
     *     the searched key (is incremented before insertion to avoid 0)
     * @param p_idOffset
     *     the lowest ChunkID in range
     * @return the value to which the key is mapped in VersionsArray
     */
    final int getVersion(final long p_key, final long p_idOffset) {
        int index = (int) ((p_key - p_idOffset) * 2);
        return getVersion(index);
    }

    /**
     * Gets the epoch at given index
     *
     * @param p_index
     *     the index
     * @return the epoch
     */
    private int getEpoch(final int p_index) {
        return m_table[p_index];
    }

    /**
     * Gets the version at given index
     *
     * @param p_index
     *     the index
     * @return the version
     */
    private int getVersion(final int p_index) {
        return m_table[p_index + 1];
    }

    /**
     * Sets the key-value tuple at given index
     *
     * @param p_index
     *     the index
     * @param p_epoch
     *     the epoch
     * @param p_version
     *     the version
     * @return whether this is a new entry or not
     */
    private boolean set(final int p_index, final int p_epoch, final int p_version) {
        boolean ret;

        ret = m_table[p_index] == -1;

        m_table[p_index] = p_epoch;
        m_table[p_index + 1] = p_version;

        return ret;
    }
}
