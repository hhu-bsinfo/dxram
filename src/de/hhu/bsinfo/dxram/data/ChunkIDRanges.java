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

package de.hhu.bsinfo.dxram.data;

import de.hhu.bsinfo.utils.ArrayListLong;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Store one or multiple chunk ID ranges. All longs are treated unsigned!
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 16.03.2017
 */
public class ChunkIDRanges implements Importable, Exportable {
    private ArrayListLong m_ranges;

    /**
     * Default constructor, contains no ranges.
     */
    public ChunkIDRanges() {
        m_ranges = new ArrayListLong(2);
    }

    /**
     * Constructor with one range
     *
     * @param p_start
     *     Start of range (including)
     * @param p_end
     *     End of range (including)
     * @throws IllegalArgumentException
     *     If p_start > p_end
     */
    public ChunkIDRanges(final long p_start, final long p_end) {
        if (!isLessThanOrEqualsUnsigned(p_start, p_end)) {
            throw new IllegalArgumentException(p_start + " > " + p_end);
        }

        m_ranges = new ArrayListLong(2);
        m_ranges.add(p_start);
        m_ranges.add(p_end);
    }

    /**
     * Constructor
     *
     * @param p_ranges
     *     Ranges to use (either copy or wrap)
     * @param p_copy
     *     True to copy the contents of the array, false to wrap the array
     * @throws IllegalArgumentException
     *     If Range size % 2 != 0
     */
    private ChunkIDRanges(final long[] p_ranges, final boolean p_copy) {
        if (p_ranges.length % 2 != 0) {
            throw new IllegalArgumentException("Ranges size % 2 != 0");
        }

        if (p_copy) {
            m_ranges = ArrayListLong.copy(p_ranges);
        } else {
            m_ranges = ArrayListLong.wrap(p_ranges);
        }
    }

    /**
     * Constructor
     *
     * @param p_arrayList
     *     ArrayListLong to use (either copy or wrap)
     * @param p_copy
     *     True to copy the contents of the list, false to wrap the list
     * @throws IllegalArgumentException
     *     If Range size % 2 != 0
     */
    private ChunkIDRanges(final ArrayListLong p_arrayList, final boolean p_copy) {
        if (p_arrayList.getSize() % 2 != 0) {
            throw new IllegalArgumentException("Ranges size % 2 != 0");
        }

        if (p_copy) {
            m_ranges = new ArrayListLong(p_arrayList);
        } else {
            m_ranges = p_arrayList;
        }
    }

    /**
     * Create a new ChunkIDRanges object based on a copy of a provided range array
     *
     * @param p_ranges
     *     Range array with contents to copy
     * @return New ChunkIDRanges object with copied contents of array
     */
    public static ChunkIDRanges copy(final long[] p_ranges) {
        return new ChunkIDRanges(p_ranges, true);
    }

    /**
     * Create a new ChunkIDRanges object wrapping an existing array with ranges
     *
     * @param p_ranges
     *     Array with ranges to wrap
     * @return ChunkIDRanges Object with wrapped array
     */
    public static ChunkIDRanges wrap(final long[] p_ranges) {
        return new ChunkIDRanges(p_ranges, false);
    }

    /**
     * Create a new ChunkIDRanges object based on a copy of a provided range list
     *
     * @param p_arrayList
     *     ArrayListLong with ranges to copy
     * @return ChunkIDRanges object with copied contents of the list
     */
    public static ChunkIDRanges copy(final ArrayListLong p_arrayList) {
        return new ChunkIDRanges(p_arrayList, true);
    }

    /**
     * Create a new ChunkIDRanges object wrapping an existing ArrayListLong with ranges
     *
     * @param p_arrayList
     *     ArrayListLong with ranges to wrap
     * @return ChunkIDRanges Object with wrapped array
     */
    public static ChunkIDRanges wrap(final ArrayListLong p_arrayList) {
        return new ChunkIDRanges(p_arrayList, false);
    }

    /**
     * Get the number of ranges
     *
     * @return Num of ranges
     */
    public int size() {
        return m_ranges.getSize() / 2;
    }

    /**
     * Check if no ranges available
     *
     * @return True if no ranges available, false otherwise
     */
    public boolean isEmpty() {
        return m_ranges.isEmpty();
    }

    /**
     * Add a range
     *
     * @param p_start
     *     Start of the range (including)
     * @param p_end
     *     End of the range (including)
     * @throws IllegalArgumentException
     *     If p_start > p_end
     */
    public void addRange(final long p_start, final long p_end) {
        if (!isLessThanOrEqualsUnsigned(p_start, p_end)) {
            throw new IllegalArgumentException(p_start + " > " + p_end);
        }

        m_ranges.add(p_start);
        m_ranges.add(p_end);
    }

    /**
     * Add all the ranges of another ChunkIDRanges instance
     *
     * @param p_other
     *     Other instance to add the ranges of
     */
    public void addAll(final ChunkIDRanges p_other) {
        m_ranges.addAll(p_other.m_ranges);
    }

    /**
     * Get the start of a range
     *
     * @param p_rangeIndex
     *     Index of the range
     * @return Start value
     */
    public long getRangeStart(final int p_rangeIndex) {
        return m_ranges.get(p_rangeIndex * 2);
    }

    /**
     * Get the end of a range
     *
     * @param p_rangeIndex
     *     Index of the range
     * @return End value
     */
    public long getRangeEnd(final int p_rangeIndex) {
        return m_ranges.get(p_rangeIndex * 2 + 1);
    }

    /**
     * Set the start of a specific range
     *
     * @param p_rangeIndex
     *     Index of the range to modify
     * @param p_val
     *     New value for the start of the range
     */
    public void setRangeStart(final int p_rangeIndex, final long p_val) {
        m_ranges.set(p_rangeIndex * 2, p_val);
    }

    /**
     * Set the end of a specific range
     *
     * @param p_rangeIndex
     *     Index of the range to modify
     * @param p_val
     *     New value for the end of the range
     */
    public void setRangeEnd(final int p_rangeIndex, final long p_val) {
        m_ranges.set(p_rangeIndex * 2 + 1, p_val);
    }

    /**
     * Check if a chunk ID is within the ranges
     *
     * @param p_chunkID
     *     Chunk ID to test
     * @return True if chunk ID is within a range (including)
     */
    public boolean isInRanges(final long p_chunkID) {
        for (int i = 0; i < m_ranges.getSize(); i += 2) {
            if (m_ranges.get(i) <= p_chunkID && p_chunkID <= m_ranges.get(i + 1)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Get a random chunk ID from a random range
     *
     * @return Random chunk ID of random range
     */
    public long getRandomChunkIdOfRanges() {
        int rangeIdx = getRandomRangeExclEnd(0, m_ranges.getSize() / 2);

        return getRandomChunkId(m_ranges.get(rangeIdx * 2), m_ranges.get(rangeIdx * 2 + 1));
    }

    /**
     * Get the total number of chunk IDs covered by the ranges
     *
     * @return Total number of chunk IDs covered
     */
    public long getTotalChunkIDsOfRanges() {
        long count = 0;

        for (int i = 0; i < m_ranges.getSize(); i += 2) {
            long rangeStart = m_ranges.get(i);
            long rangeEnd = m_ranges.get(i + 1);

            count = rangeEnd - rangeStart + 1;
        }

        return count;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.exportObject(m_ranges);
    }

    @Override
    public void importObject(final Importer p_importer) {
        p_importer.importObject(m_ranges);
    }

    @Override
    public int sizeofObject() {
        return m_ranges.sizeofObject();
    }

    @Override
    public String toString() {
        String str = "";

        for (int i = 0; i < m_ranges.getSize(); i += 2) {
            str += String.format("[0x%X, 0x%X]", m_ranges.get(i), m_ranges.get(i + 1));
        }

        return str;
    }

    /**
     * Get a random chunk ID from a range
     *
     * @param p_start
     *     Start of the range (including)
     * @param p_end
     *     End of the range (including)
     * @return Random chunk ID of range
     */
    private static long getRandomChunkId(final long p_start, final long p_end) {
        if (ChunkID.getCreatorID(p_start) != ChunkID.getCreatorID(p_end)) {
            return ChunkID.INVALID_ID;
        }

        return getRandomRange(p_start, p_end);
    }

    /**
     * Get a random range
     *
     * @param p_start
     *     Start (including)
     * @param p_end
     *     End (including)
     * @return Random range
     */
    private static int getRandomRange(final int p_start, final int p_end) {
        if (p_start == p_end) {
            return p_start;
        }

        return (int) (Math.random() * (p_end - p_start + 1) + p_start);
    }

    /**
     * Get a random range excluding the end
     *
     * @param p_start
     *     Start (including)
     * @param p_end
     *     End (excluding)
     * @return Random range
     */
    private static int getRandomRangeExclEnd(final int p_start, final int p_end) {
        return (int) (Math.random() * (p_end - p_start) + p_start);
    }

    /**
     * Get a random range
     *
     * @param p_start
     *     Start (including)
     * @param p_end
     *     End (including)
     * @return Random range
     */
    private static long getRandomRange(final long p_start, final long p_end) {
        if (p_start == p_end) {
            return p_start;
        }

        long tmp = (long) (Math.random() * (p_end - p_start + 1));
        return tmp + p_start;
    }

    /**
     * Unsigned comparison of two long values
     *
     * @param p_n1
     *     Value 1
     * @param p_n2
     *     Value 2
     * @return True if p_n1 <= p_n2
     */
    private static boolean isLessThanOrEqualsUnsigned(final long p_n1, final long p_n2) {
        return p_n1 <= p_n2 ^ p_n1 < 0 != p_n2 < 0;
    }
}
