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

import java.util.Arrays;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * Custom array list implementation offering direct access to a primitive long array
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.01.2017
 */
public class ArrayListLong implements Importable, Exportable {

    private int m_capacityChunk = 10;
    private long[] m_array;
    private int m_size = 0;

    /**
     * Default constructor
     */
    public ArrayListLong() {
        m_array = new long[m_capacityChunk];
        Arrays.fill(m_array, ChunkID.INVALID_ID);
    }

    /**
     * Create the array list with a specific capacity chunk size
     *
     * @param p_capacityChunk
     *         capacity chunk size
     */
    public ArrayListLong(final int p_capacityChunk) {
        m_capacityChunk = p_capacityChunk;
        m_array = new long[p_capacityChunk];
        Arrays.fill(m_array, ChunkID.INVALID_ID);
    }

    /**
     * Create the array list with a single element inserted on construction
     *
     * @param p_element
     *         Element to insert on construction
     */
    public ArrayListLong(final long p_element) {
        m_array = new long[] {p_element};
        m_size = 1;
    }

    /**
     * Copy constructor
     *
     * @param p_list
     *         Contents of list to copy
     */
    public ArrayListLong(final ArrayListLong p_list) {
        m_array = new long[p_list.m_size];
        System.arraycopy(p_list.m_array, 0, m_array, 0, m_array.length);
    }

    /**
     * Constructor for wrapper method
     *
     * @param p_array
     *         Array to wrap
     */
    private ArrayListLong(final long[] p_array) {
        m_array = p_array;
        m_size = p_array.length;
    }

    /**
     * Constructor for copying the contents of a provided array
     *
     * @param p_array
     *         Array with contents to copy
     * @param p_dummy
     *         Dummy variable
     */
    private ArrayListLong(final long[] p_array, boolean p_dummy) {
        m_array = Arrays.copyOf(p_array, p_array.length);
        m_size = p_array.length;
    }

    /**
     * Wrap an existing primitive long aray
     *
     * @param p_array
     *         Array to wrap
     * @return ArrayListLong object with wrapped array
     */
    public static ArrayListLong wrap(final long[] p_array) {
        return new ArrayListLong(p_array);
    }

    /**
     * Create a array list with the contents of the provided primitive array
     *
     * @param p_array
     *         Array with contents to copy
     * @return ArrayListLong object with contents of provided array
     */
    public static ArrayListLong copy(final long[] p_array) {
        return new ArrayListLong(p_array, true);
    }

    /**
     * Get the size (number of inserted elements NOT capacity) of the array
     *
     * @return Size of the array
     */
    public int getSize() {
        return m_size;
    }

    /**
     * Check if the array ist empty
     *
     * @return True on empty, false otherwise
     */
    public boolean isEmpty() {
        return m_size == 0;
    }

    /**
     * Get the underlying primitive long array
     *
     * @return Primitive long array
     */
    public long[] getArray() {
        return m_array;
    }

    /**
     * Add an element to the array. The array is automatically resized if necessary
     *
     * @param p_val
     *         Value to add
     */
    public void add(final long p_val) {
        if (m_array.length - m_size == 0) {
            m_array = Arrays.copyOf(m_array, m_array.length + m_capacityChunk);
            Arrays.fill(m_array, m_size, m_array.length, ChunkID.INVALID_ID);
        }

        m_array[m_size++] = p_val;
    }

    /**
     * Add an element to the array at given index. The array is automatically resized if necessary
     * Do not mix with add(long)!
     *
     * @param p_index
     *         index at which the specified element is to be inserted
     * @param p_val
     *         Value to add
     */
    public void add(final int p_index, final long p_val) {
        if (m_array.length <= p_index) {
            int oldSize = m_array.length;
            int newSize = p_index / m_capacityChunk * m_capacityChunk + m_capacityChunk;
            m_array = Arrays.copyOf(m_array, newSize);
            Arrays.fill(m_array, oldSize, m_array.length, ChunkID.INVALID_ID);
        }

        m_array[p_index] = p_val;
        m_size++;
    }

    /**
     * Add all values of another ArrayListLong object
     *
     * @param p_list
     *         Array with elements to add to the current one
     */
    public void addAll(final ArrayListLong p_list) {
        m_array = Arrays.copyOf(m_array, m_array.length + p_list.m_size);
        System.arraycopy(p_list.m_array, 0, m_array, m_size, p_list.m_size);
        m_size += p_list.m_size;
    }

    /**
     * Replaces the element at given index
     *
     * @param p_index
     *         index at which the specified element is to be overwritten
     * @param p_val
     *         Value to set
     */
    public void set(final int p_index, final long p_val) {
        m_array[p_index] = p_val;
    }

    /**
     * Get an element from the array
     *
     * @param p_index
     *         Index to access
     * @return Element at the specified index
     */
    public long get(final int p_index) {
        return m_array[p_index];
    }

    /**
     * Remove an element from the array
     *
     * @param p_index
     *         Index of the element to remove
     * @return Removed value
     */
    public long remove(final int p_index) {
        long oldValue = m_array[p_index];

        int numMoved = m_size - p_index - 1;
        if (numMoved > 0) {
            System.arraycopy(m_array, p_index + 1, m_array, p_index, numMoved);
        }

        m_size--;

        return oldValue;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeCompactNumber(m_size);
        p_exporter.writeLongs(m_array, 0, m_size);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_array = p_importer.readLongArray(m_array);
        m_size = m_array.length;
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofCompactedNumber(m_size) + Long.BYTES * m_size;
    }
}
