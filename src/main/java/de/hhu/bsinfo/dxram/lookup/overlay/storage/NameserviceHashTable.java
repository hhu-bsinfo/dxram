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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.lookup.overlay.OverlayHelper;
import de.hhu.bsinfo.dxutils.CRC16;
import de.hhu.bsinfo.dxutils.hashtable.HashFunctionCollection;
import de.hhu.bsinfo.dxutils.hashtable.IntLongHashTable;

/**
 * HashTable to store ID-Mappings (Linear probing)
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 27.01.2014
 */
public class NameserviceHashTable extends IntLongHashTable implements MetadataInterface {

    private static final Logger LOGGER = LogManager.getFormatterLogger(NameserviceHashTable.class);

    /**
     * Creates an instance of IDHashTable
     *
     * @param p_initialElementCapacity
     *         the initial capacity of IDHashTable
     */
    public NameserviceHashTable(final int p_initialElementCapacity) {
        super(p_initialElementCapacity);
    }

    /**
     * Converts an byte array with all entries to an ArrayList with Pairs.
     *
     * @param p_array
     *         all serialized nameservice entries
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

    @Override
    public int storeMetadata(final byte[] p_data, final int p_offset, final int p_size) {
        int ret = 0;
        ByteBuffer data;

        if (p_data != null) {
            data = ByteBuffer.wrap(p_data, p_offset, p_size);

            for (int i = 0; i < data.limit() / 12; i++) {

                LOGGER.trace("Storing nameservice entry");

                putChunkID(data.getInt(), data.getLong());
                ret++;
            }
        }

        return ret;
    }

    @Override
    public byte[] receiveAllMetadata() {
        ByteBuffer data;
        int iter;

        data = ByteBuffer.allocate(size() * 12);

        for (int i = 0; i < capacity(); i++) {
            iter = getKey(i);
            if (iter != 0) {

                LOGGER.trace("Including nameservice entry: %s <-> %s", iter - 1, getValue(i));

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

        data = ByteBuffer.allocate(size() * 12);

        for (int i = 0; i < capacity(); i++) {
            iter = getKey(i);
            if (iter != 0) {
                if (OverlayHelper.isHashInSuperpeerRange(CRC16.hash(iter - 1), p_bound1, p_bound2)) {

                    LOGGER.trace("Including nameservice entry: %s <-> %s", iter - 1, getValue(i));

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

        for (int i = 0; i < capacity(); i++) {
            iter = getKey(i);
            if (iter != 0) {
                if (!OverlayHelper.isHashInSuperpeerRange(CRC16.hash(iter - 1), p_bound1, p_bound2)) {

                    LOGGER.trace("Removing nameservice entry: %s <-> %s", iter - 1, getValue(i));

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

        for (int i = 0; i < capacity(); i++) {
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
     *         the searched key (is incremented before insertion to avoid 0)
     * @return the value to which the key is mapped in IDHashTable
     */
    public final long getChunkID(final int p_key) {
        return get(p_key + 1);
    }

    /**
     * Maps the given key to the given value in IDHashTable
     *
     * @param p_key
     *         the key (is incremented before insertion to avoid 0)
     * @param p_value
     *         the value
     */
    final void putChunkID(final int p_key, final long p_value) {
        put(p_key + 1, p_value);
    }

    /**
     * Removes the given key from IDHashTable
     *
     * @param p_key
     *         the key (is incremented before insertion to avoid 0)
     * @return the value
     */
    public final long remove(final int p_key) {
        long ret = -1;
        int index;
        int iter;
        final int key = p_key + 1;

        index = (HashFunctionCollection.hash(key) & 0x7FFFFFFF) % capacity();

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
            putChunkID(iter, getValue(index));

            iter = getKey(++index);
        }

        return ret;
    }

    /**
     * Print all tuples in IDHashTable
     */
    public final void print() {
        int iter;

        for (int i = 0; i < capacity(); i++) {
            iter = getKey(i);
            if (iter != 0) {
                System.out.println("Key: " + iter + ", value: " + ChunkID.toHexString(getValue(i)));
            }
        }
    }
}
