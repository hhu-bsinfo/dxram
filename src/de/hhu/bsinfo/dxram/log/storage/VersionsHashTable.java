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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * HashTable to store versions (Linear probing)
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 23.02.2014
 */
class VersionsHashTable {

    private static final Logger LOGGER = LogManager.getFormatterLogger(VersionsHashTable.class.getSimpleName());

    private int[] m_table;
    private int m_count;
    private int m_elementCapacity;

    // Constructors

    /**
     * Creates an instance of VersionsHashTable
     *
     * @param p_initialElementCapacity
     *     the initial capacity of VersionsHashTable
     */
    VersionsHashTable(final int p_initialElementCapacity) {
        super();

        m_count = 0;
        m_elementCapacity = p_initialElementCapacity;
        if (p_initialElementCapacity == 0) {
            m_elementCapacity = 100;
        }

        m_table = new int[m_elementCapacity * 4];
    }

    // Getter / Setter

    /**
     * Clears VersionsHashTable
     */
    public final void clear() {
        if (m_count != 0) {
            Arrays.fill(m_table, 0);
            m_count = 0;
        }
    }

    /**
     * Returns all entries
     *
     * @return the array
     */
    protected final int[] getTable() {
        return m_table;
    }

    /**
     * Returns the number of keys in VersionsHashTable
     *
     * @return the number of keys in VersionsHashTable
     */
    protected final int size() {
        return m_count;
    }

    // Methods

    /**
     * Returns the number of keys fitting in VersionsHashTable
     *
     * @return the number of keys fitting in VersionsHashTable
     */
    protected final int capacity() {
        return m_table.length / 4;
    }

    /**
     * Returns the value to which the specified key is mapped in VersionsHashTable
     *
     * @param p_key
     *     the searched key (is incremented before insertion to avoid 0)
     * @return the value to which the key is mapped in VersionsHashTable
     */
    protected final Version get(final long p_key) {
        Version ret = null;
        int index;
        long iter;
        final long key = p_key + 1;

        index = (VersionsBuffer.hash(key) & 0x7FFFFFFF) % m_elementCapacity;

        iter = getKey(index);
        while (iter != 0) {
            if (iter == key) {
                ret = new Version((short) getEpoch(index), getVersion(index));
                break;
            }
            iter = getKey(++index);
        }

        return ret;
    }

    /**
     * Maps the given key to the given value in VersionsHashTable
     *
     * @param p_key
     *     the key (is incremented before insertion to avoid 0)
     * @param p_epoch
     *     the epoch
     * @param p_version
     *     the version
     */
    protected void put(final long p_key, final int p_epoch, final int p_version) {
        int index;
        long iter;
        final long key = p_key + 1;

        if (m_count > m_elementCapacity * 0.9) {
            rehash();
        }

        index = (VersionsBuffer.hash(key) & 0x7FFFFFFF) % m_elementCapacity;

        iter = getKey(index);
        while (iter != 0) {
            if (iter == key) {
                set(index, key, p_epoch, p_version);
                break;
            }
            iter = getKey(++index);
        }
        if (iter == 0) {
            // Key unknown until now
            set(index, key, p_epoch, p_version);
            m_count++;
        }
    }

    /**
     * Gets the key at given index
     *
     * @param p_index
     *     the index
     * @return the key
     */
    private long getKey(final int p_index) {
        int index;

        index = p_index % m_elementCapacity * 4;
        return (long) m_table[index] << 32 | m_table[index + 1] & 0xFFFFFFFFL;
    }

    /**
     * Gets the epoch at given index
     *
     * @param p_index
     *     the index
     * @return the epoch
     */
    private int getEpoch(final int p_index) {
        return m_table[p_index % m_elementCapacity * 4 + 2];
    }

    /**
     * Gets the version at given index
     *
     * @param p_index
     *     the index
     * @return the version
     */
    private int getVersion(final int p_index) {
        return m_table[p_index % m_elementCapacity * 4 + 3];
    }

    /**
     * Sets the key-value tuple at given index
     *
     * @param p_index
     *     the index
     * @param p_key
     *     the key
     * @param p_epoch
     *     the epoch
     * @param p_version
     *     the version
     */
    private void set(final int p_index, final long p_key, final int p_epoch, final int p_version) {
        int index;

        index = p_index % m_elementCapacity * 4;
        m_table[index] = (int) (p_key >> 32);
        m_table[index + 1] = (int) p_key;
        m_table[index + 2] = p_epoch;
        m_table[index + 3] = p_version;
    }

    /**
     * Increases the capacity of and internally reorganizes hashtable
     */
    private void rehash() {
        int index = 0;
        int oldCount;
        int oldElementCapacity;
        int[] oldTable;
        int[] newTable;

        oldCount = m_count;
        oldElementCapacity = m_elementCapacity;
        oldTable = m_table;

        m_elementCapacity = m_elementCapacity * 2 + 1;
        newTable = new int[m_elementCapacity * 4];
        m_table = newTable;

        // #if LOGGER == TRACE
        LOGGER.trace("Reached threshold -> Rehashing. New size: %d... ", m_elementCapacity);
        // #endif /* LOGGER == TRACE */

        m_count = 0;
        while (index < oldElementCapacity) {
            if (oldTable[index * 4] != 0) {
                put(((long) oldTable[index] << 32 | oldTable[index + 1] & 0xFFFFFFFFL) - 1, oldTable[index + 2], oldTable[index + 3]);
            }
            index = (index + 1) % m_elementCapacity;
        }
        m_count = oldCount;

        // #if LOGGER == TRACE
        LOGGER.trace("done");
        // #endif /* LOGGER == TRACE */
    }
}
