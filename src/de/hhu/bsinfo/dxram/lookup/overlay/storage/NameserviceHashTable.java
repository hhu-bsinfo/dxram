/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.lookup.overlay.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.lookup.overlay.OverlayHelper;
import de.hhu.bsinfo.dxutils.CRC16;

/**
 * HashTable to store ID-Mappings (Linear probing)
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 27.01.2014
 */
public class NameserviceHashTable extends AbstractMetadata {

    private static final Logger LOGGER = LogManager.getFormatterLogger(NameserviceHashTable.class.getSimpleName());

    // Attributes
    private int[] m_table;
    private int m_count;
    private int m_elementCapacity;
    private int m_threshold;
    private float m_loadFactor;

    // Constructors

    /**
     * Creates an instance of IDHashTable
     *
     * @param p_initialElementCapacity
     *     the initial capacity of IDHashTable
     * @param p_loadFactor
     *     the load factor of IDHashTable
     */
    public NameserviceHashTable(final int p_initialElementCapacity, final float p_loadFactor) {
        super();

        m_count = 0;
        m_elementCapacity = p_initialElementCapacity;
        m_loadFactor = p_loadFactor;

        if (m_elementCapacity == 0) {
            m_table = new int[3];
            m_threshold = (int) m_loadFactor;
        } else {
            m_table = new int[m_elementCapacity * 3];
            m_threshold = (int) (m_elementCapacity * m_loadFactor);
        }
    }

    /**
     * Converts an byte array with all entries to an ArrayList with Pairs.
     *
     * @param p_array
     *     all serialized nameservice entries
     * @return Array list with entries as pairs of index + value
     */
    public static ArrayList<NameserviceEntry> convert(final byte[] p_array) {
        ArrayList<NameserviceEntry> ret;
        int count = p_array.length / 12;
        ByteBuffer buffer = ByteBuffer.wrap(p_array);

        ret = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ret.add(new NameserviceEntry(buffer.getInt(i * 12), buffer.getLong(i * 12 + 4)));
        }
        return ret;
    }

    /**
     * Hashes the given key
     *
     * @param p_key
     *     the key
     * @return the hash value
     */
    private static int hash(final int p_key) {
        int hash = p_key;

        hash = (hash >> 16 ^ hash) * 0x45d9f3b;
        hash = (hash >> 16 ^ hash) * 0x45d9f3b;
        return hash >> 16 ^ hash;
        /*
         * hash ^= (hash >>> 20) ^ (hash >>> 12);
         * return hash ^ (hash >>> 7) ^ (hash >>> 4);
         */
    }

    @Override
    public int storeMetadata(final byte[] p_data, final int p_offset, final int p_size) {
        int ret = 0;
        ByteBuffer data;

        if (p_data != null) {
            data = ByteBuffer.wrap(p_data, p_offset, p_size);

            for (int i = 0; i < data.limit() / 12; i++) {
                // #if LOGGER == TRACE
                LOGGER.trace("Storing nameservice entry");
                // #endif /* LOGGER == TRACE */

                put(data.getInt(), data.getLong());
                ret++;
            }
        }

        return ret;
    }

    @Override
    public byte[] receiveAllMetadata() {
        ByteBuffer data;
        int iter;

        data = ByteBuffer.allocate(m_count * 12);

        for (int i = 0; i < m_elementCapacity; i++) {
            iter = getKey(i);
            if (iter != 0) {
                // #if LOGGER == TRACE
                LOGGER.trace("Including nameservice entry: %s <-> %s", iter - 1, getValue(i));
                // #endif /* LOGGER == TRACE */

                data.putInt(iter - 1);
                data.putLong(getValue(i));
            }
        }
        return data.array();
    }

    @Override
    public byte[] receiveMetadataInRange(final short p_bound1, final short p_bound2) {
        int count = 0;
        int iter;
        ByteBuffer data;

        data = ByteBuffer.allocate(m_count * 12);

        for (int i = 0; i < m_elementCapacity; i++) {
            iter = getKey(i);
            if (iter != 0) {
                if (OverlayHelper.isHashInSuperpeerRange(CRC16.hash(iter - 1), p_bound1, p_bound2)) {
                    // #if LOGGER == TRACE
                    LOGGER.trace("Including nameservice entry: %s <-> %s", iter - 1, getValue(i));
                    // #endif /* LOGGER == TRACE */

                    data.putInt(iter - 1);
                    data.putLong(getValue(i));
                    count++;
                }
            }
        }
        return Arrays.copyOfRange(data.array(), 0, count * (Integer.BYTES + Long.BYTES));
    }

    @Override
    public int removeMetadataOutsideOfRange(final short p_bound1, final short p_bound2) {
        int count = 0;
        int iter;

        for (int i = 0; i < m_elementCapacity; i++) {
            iter = getKey(i);
            if (iter != 0) {
                if (!OverlayHelper.isHashInSuperpeerRange(CRC16.hash(iter - 1), p_bound1, p_bound2)) {
                    // #if LOGGER == TRACE
                    LOGGER.trace("Removing nameservice entry: %s <-> %s", iter - 1, getValue(i));
                    // #endif /* LOGGER == TRACE */

                    count++;
                    remove(iter);
                    // Try this index again as removing might have filled this slot with different data
                    i--;
                }
            }
        }

        return count;
    }

    @Override
    public int quantifyMetadata(final short p_bound1, final short p_bound2) {
        int count = 0;
        int iter;

        for (int i = 0; i < m_elementCapacity; i++) {
            iter = getKey(i);
            if (iter != 0) {
                if (OverlayHelper.isHashInSuperpeerRange(CRC16.hash(iter - 1), p_bound1, p_bound2)) {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * Returns the value to which the specified key is mapped in IDHashTable
     *
     * @param p_key
     *     the searched key (is incremented before insertion to avoid 0)
     * @return the value to which the key is mapped in IDHashTable
     */
    public final long get(final int p_key) {
        long ret = 0;
        int index;
        int iter;
        final int key = p_key + 1;

        index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

        iter = getKey(index);
        while (iter != 0) {
            if (iter == key) {
                ret = getValue(index);
                break;
            }
            iter = getKey(++index);
        }

        return ret;
    }

    /**
     * Maps the given key to the given value in IDHashTable
     *
     * @param p_key
     *     the key (is incremented before insertion to avoid 0)
     * @param p_value
     *     the value
     * @return the old value
     */
    public final long put(final int p_key, final long p_value) {
        long ret = -1;
        int index;
        int iter;
        final int key = p_key + 1;

        index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

        iter = getKey(index);
        while (iter != 0) {
            if (iter == key) {
                ret = getValue(index);
                set(index, key, p_value);
                break;
            }
            iter = getKey(++index);
        }
        if (ret == -1) {
            set(index, key, p_value);
            m_count++;
        }

        if (m_count >= m_threshold) {
            rehash();
        }

        return ret;
    }

    /**
     * Removes the given key from IDHashTable
     *
     * @param p_key
     *     the key (is incremented before insertion to avoid 0)
     * @return the value
     */
    public final long remove(final int p_key) {
        long ret = -1;
        int index;
        int iter;
        final int key = p_key + 1;

        index = (hash(key) & 0x7FFFFFFF) % m_elementCapacity;

        iter = getKey(index);
        while (iter != 0) {
            if (iter == key) {
                ret = getValue(index);
                set(index, 0, 0);
                break;
            }
            iter = getKey(++index);
        }

        iter = getKey(++index);
        while (iter != 0) {
            set(index, 0, 0);
            put(iter, getValue(index));

            iter = getKey(++index);
        }

        return ret;
    }

    /**
     * Print all tuples in IDHashTable
     */
    public final void print() {
        int iter;

        for (int i = 0; i < m_elementCapacity; i++) {
            iter = getKey(i);
            if (iter != 0) {
                System.out.println("Key: " + iter + ", value: " + ChunkID.toHexString(getValue(i)));
            }
        }
    }

    /**
     * Print all tuples in IDHashTable sorted
     */
    public final void printSorted() {
        int iter;
        Collection<Entry> list;

        list = new TreeSet<>(Comparator.comparingInt(p_entryA -> p_entryA.m_key));

        for (int i = 0; i < m_elementCapacity; i++) {
            iter = getKey(i);
            if (iter != 0) {
                list.add(new Entry(iter, getValue(i)));
            }
        }

        for (Entry entry : list) {
            System.out.println("Key: " + entry.m_key + ", value: " + ChunkID.toHexString(entry.m_value));
        }
    }

    /**
     * Sets the key-value tuple at given index
     *
     * @param p_index
     *     the index
     * @param p_key
     *     the key
     * @param p_value
     *     the value
     */
    private void set(final int p_index, final int p_key, final long p_value) {
        int index;

        index = p_index % m_elementCapacity * 3;
        m_table[index] = p_key;
        m_table[index + 1] = (int) (p_value >> 32);
        m_table[index + 2] = (int) p_value;
    }

    /**
     * Gets the key at given index
     *
     * @param p_index
     *     the index
     * @return the key
     */
    private int getKey(final int p_index) {
        return m_table[p_index % m_elementCapacity * 3];
    }

    /**
     * Gets the value at given index
     *
     * @param p_index
     *     the index
     * @return the value
     */
    private long getValue(final int p_index) {
        int index;

        index = p_index % m_elementCapacity * 3 + 1;
        return (long) m_table[index] << 32 | m_table[index + 1] & 0xFFFFFFFFL;
    }

    /**
     * Increases the capacity of and internally reorganizes IDHashTable
     */
    private void rehash() {
        int index = 0;
        int oldCount;
        int oldElementCapacity;
        int oldThreshold;
        int[] oldTable;
        int[] newTable;

        oldCount = m_count;
        oldElementCapacity = m_elementCapacity;
        oldThreshold = m_threshold;
        oldTable = m_table;

        m_elementCapacity = m_elementCapacity * 2 + 1;
        newTable = new int[m_elementCapacity * 3];
        m_threshold = (int) (m_elementCapacity * m_loadFactor);
        m_table = newTable;

        // #if LOGGER == TRACE
        LOGGER.trace("Reached threshold (%d) -> Rehashing. New size: %d... ", oldThreshold, m_elementCapacity);
        // #endif /* LOGGER == TRACE */

        m_count = 0;
        while (index < oldElementCapacity) {
            if (oldTable[index * 3] != 0) {
                put(oldTable[index * 3] - 1, (long) oldTable[index * 3 + 1] << 32 | oldTable[index * 3 + 2] & 0xFFFFFFFFL);
            }
            index = (index + 1) % m_elementCapacity;
        }
        m_count = oldCount;
        // #if LOGGER == TRACE
        LOGGER.trace("done");
        // #endif /* LOGGER == TRACE */
    }

    /**
     * A single Entry in IDHashTable
     */
    private static class Entry {

        // Attributes
        private int m_key;
        private long m_value;

        // Constructors

        /**
         * Creates an instance of Entry
         *
         * @param p_key
         *     the key
         * @param p_value
         *     the value
         */
        protected Entry(final int p_key, final long p_value) {
            m_key = p_key;
            m_value = p_value;
        }
    }
}
