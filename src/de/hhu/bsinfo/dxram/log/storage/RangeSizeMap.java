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

package de.hhu.bsinfo.dxram.log.storage;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RangeSizeMap {

    private static final int INITIAL_SIZE = 100;
    private static final float LOAD_FACTOR = 0.9f;

    private static final Logger LOGGER = LogManager.getFormatterLogger(RangeSizeMap.class.getSimpleName());

    // Attributes
    private int[] m_table;
    private int m_elementCapacity;
    private int m_count;

    private ArrayList<int[]> m_list;

    // Constructors

    /**
     * Creates an instance of RangeSizeMap
     */
    RangeSizeMap() {
        m_count = 0;
        m_elementCapacity = INITIAL_SIZE;

        m_table = new int[m_elementCapacity * 2];
        m_list = new ArrayList<int[]>(m_elementCapacity);
    }

    /**
     * Returns the size of the map
     *
     * @return the number of entries in the hash table
     */
    public int size() {
        return m_count;
    }

    /**
     * Converts an byte array with all entries to an ArrayList with pairs.
     *
     * @return View on ArrayList with entries as pairs of index + value (int array)
     */
    public List<int[]> convert() {

        int count = 0;
        for (int i = 0; i < m_elementCapacity; i++) {
            int key = m_table[i * 2];
            if (key != 0) {
                if (m_list.size() > count) {
                    int[] arr = m_list.get(count);
                    arr[0] = key;
                    arr[1] = m_table[i * 2 + 1];
                } else {
                    int[] arr = new int[2];
                    arr[0] = key;
                    arr[1] = m_table[i * 2 + 1];
                    m_list.add(arr);
                }
                count++;
            }
        }

        return m_list.subList(0, m_count);
    }

    /**
     * Hashes the given key
     *
     * @param p_key
     *         the key
     * @return the hash value
     */
    private static int hash(final int p_key) {
        int hash = p_key;

        hash = (hash >> 16 ^ hash) * 0x45d9f3b;
        hash = (hash >> 16 ^ hash) * 0x45d9f3b;
        return hash >> 16 ^ hash;
    }

    /**
     * Returns the value to which the specified key is mapped in RangeSizeMap
     *
     * @param p_key
     *         the searched key
     * @return the value to which the key is mapped in RangeSizeMap
     */
    public final int get(final int p_key) {
        int ret = 0;
        int index;
        int iter;

        index = (hash(p_key) & 0x7FFFFFFF) % m_elementCapacity;

        iter = getKey(index);
        while (iter != 0) {
            if (iter == p_key) {
                ret = getValue(index);
                break;
            }
            iter = getKey(++index);
        }

        return ret;
    }

    /**
     * Maps the given key to the given value in RangeSizeMap
     *
     * @param p_key
     *         the key (is incremented before insertion to avoid 0)
     * @param p_value
     *         the value
     * @return the old value
     */
    public final int add(final int p_key, final int p_value) {
        int ret = -1;
        int index;
        int iter;

        index = (hash(p_key) & 0x7FFFFFFF) % m_elementCapacity;

        iter = getKey(index);
        while (iter != 0) {
            if (iter == p_key) {
                ret = getValue(index);
                set(index, p_key, ret + p_value);
                break;
            }
            iter = getKey(++index);
        }
        if (ret == -1) {
            set(index, p_key, p_value);
            m_count++;
        }

        if (m_count >= m_elementCapacity * LOAD_FACTOR) {
            rehash();
        }

        return ret;
    }

    public final void clear() {
        int length = m_table.length;

        /* The array and list is never truncated as the maximum number of concurrently accessed
         backup zones is rather low */

        // This is faster than Arrays.fill
        m_table[0] = 0;
        for (int i = 1; i < length; i += i) {
            System.arraycopy(m_table, 0, m_table, i, length - i < i ? length - i : i);
        }

        m_count = 0;
    }

    /**
     * Sets the key-value tuple at given index
     *
     * @param p_index
     *         the index
     * @param p_key
     *         the key
     * @param p_value
     *         the value
     */
    private void set(final int p_index, final int p_key, final int p_value) {
        int index;

        index = p_index % m_elementCapacity * 2;
        m_table[index] = p_key;
        m_table[index + 1] = p_value;
    }

    /**
     * Gets the key at given index
     *
     * @param p_index
     *         the index
     * @return the key
     */
    private int getKey(final int p_index) {
        return m_table[p_index % m_elementCapacity * 2];
    }

    /**
     * Gets the value at given index
     *
     * @param p_index
     *         the index
     * @return the value
     */
    private int getValue(final int p_index) {
        return m_table[p_index % m_elementCapacity * 2 + 1];
    }

    /**
     * Increases the capacity of and internally reorganizes RangeSizeMap
     */
    private void rehash() {
        int index = 0;
        int oldCount;
        int oldElementCapacity;
        int[] oldTable;
        int[] newTable;

        // #if LOGGER == TRACE
        LOGGER.trace("Re-hashing (count:  %d)", m_count);
        // #endif /* LOGGER == TRACE */

        oldCount = m_count;
        oldElementCapacity = m_elementCapacity;
        oldTable = m_table;

        m_elementCapacity = m_elementCapacity * 2 + 1;
        newTable = new int[m_elementCapacity * 2];
        m_table = newTable;

        m_count = 0;
        while (index < oldElementCapacity) {
            if (oldTable[index * 2] != 0) {
                add(oldTable[index * 2], oldTable[index * 2 + 1]);
            }
            index = (index + 1) % m_elementCapacity;
        }
        m_count = oldCount;
    }

}
