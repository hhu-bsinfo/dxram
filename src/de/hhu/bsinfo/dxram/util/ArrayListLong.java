package de.hhu.bsinfo.dxram.util;

import java.util.Arrays;

import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Custom array list implementation offering direct access to a primitive long array
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.01.2017
 */
public class ArrayListLong implements Importable, Exportable {

    private static final int CAPACITY_CHUNK = 10;

    private long[] m_array;
    private int m_size;

    /**
     * Wrap an existing primitive long aray
     *
     * @param p_array
     *     Array to wrap
     * @return ArrayListLong object with wrapped array
     */
    public static ArrayListLong wrap(final long[] p_array) {
        return new ArrayListLong(p_array);
    }

    /**
     * Default constructor
     */
    public ArrayListLong() {
        m_array = new long[CAPACITY_CHUNK];
    }

    /**
     * Create the array list with a single element inserted on construction
     *
     * @param p_element
     *     Element to insert on construction
     */
    public ArrayListLong(final long p_element) {
        m_array = new long[] {p_element};
        m_size = 1;
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
     *     Value to add
     */
    public void add(final long p_val) {
        if (m_array.length - m_size == 0) {
            m_array = Arrays.copyOf(m_array, m_array.length + CAPACITY_CHUNK);
        }

        m_array[m_size++] = p_val;
    }

    /**
     * Add all values of another ArrayListLong object
     *
     * @param p_list
     *     Array with elements to add to the current one
     */
    public void addAll(final ArrayListLong p_list) {
        m_array = Arrays.copyOf(m_array, m_array.length + p_list.m_size);
        System.arraycopy(p_list.m_array, 0, m_array, m_size, p_list.m_size);
        m_size += p_list.m_size;
    }

    /**
     * Get an element from the array
     *
     * @param p_index
     *     Index to access
     * @return Element at the specified index
     */
    public long get(final int p_index) {
        return m_array[p_index];
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_size);
        p_exporter.writeLongs(m_array, 0, m_size);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_array = p_importer.readLongArray();
        m_size = m_array.length;
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES + Long.BYTES * m_size;
    }

    /**
     * Constructor for wrapper method
     *
     * @param p_array
     *     Array to wrap
     */
    private ArrayListLong(final long[] p_array) {
        m_array = p_array;
        m_size = p_array.length;
    }
}
