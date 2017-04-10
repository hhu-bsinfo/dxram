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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;

/**
 * Implementation of a Bloom-filter
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 26.05.2013
 */
public final class BloomFilter {

    // Attributes
    private BitSet m_bitset;
    private int m_sizeOfBitset;
    private int m_k;

    private static MessageDigest ms_digestFunction;

    static {
        try {
            ms_digestFunction = java.security.MessageDigest.getInstance("MD5");
        } catch (final NoSuchAlgorithmException e) {
            ms_digestFunction = null;
        }
    }

    // Constructors

    /**
     * Creates an instance of BloomFilter
     *
     * @param p_sizeOfBitset
     *     defines how many bytes should be used in total for the filter
     * @param p_expectedNumberOfElements
     *     defines the maximum number of elements the filter is expected to contain
     */
    public BloomFilter(final int p_sizeOfBitset, final int p_expectedNumberOfElements) {
        final int sizeInBits = p_sizeOfBitset * 8;
        int n;
        double d;

        d = sizeInBits / (double) p_expectedNumberOfElements;
        n = p_expectedNumberOfElements;

        m_k = (int) Math.round(sizeInBits / (double) p_expectedNumberOfElements * Math.log(2.0));
        m_sizeOfBitset = (int) Math.ceil(d * n);
        m_bitset = new BitSet(sizeInBits);
    }

    // Methods

    /**
     * Generates digests based on the contents of an array of bytes and splits the result into 4-byte int's and
     * store them in an array. The digest function is called until the required number of int's are produced. For
     * each call to digest a salt is prepended to the data. The salt is increased by 1 for each call
     *
     * @param p_data
     *     specifies input data
     * @param p_hashes
     *     number of hashes/int's to produce
     * @return array of int-sized hashes
     */
    private static int[] createHashes(final byte[] p_data, final int p_hashes) {
        int k;
        int h;
        byte salt;
        int[] result;
        byte[] digest;

        k = 0;
        h = 0;
        salt = 0;
        result = new int[p_hashes];

        while (k < p_hashes) {
            ms_digestFunction.update(salt++);
            digest = ms_digestFunction.digest(p_data);

            for (int i = 0; i < digest.length / 4 && k < p_hashes; i++) {
                h = 0;
                for (int j = i * 4; j < i * 4 + 4; j++) {
                    h <<= 8;
                    h |= digest[j] & 0xFF;
                }
                result[k] = h;
                k++;
            }
        }
        return result;
    }

    /**
     * Adds an object to the bloom filter
     *
     * @param p_element
     *     is an element (short) to register in the bloom filter
     */
    public void add(final Short p_element) {
        byte[] bytes;
        int[] hashes;

        bytes = new byte[2];
        bytes[0] = (byte) (p_element & 0xff);
        bytes[1] = (byte) (p_element >> 8 & 0xff);

        hashes = createHashes(bytes, m_k);
        for (int hash : hashes) {
            m_bitset.set(Math.abs(hash % m_sizeOfBitset), true);
        }
    }

    /**
     * Verifies if given element is in bloom filter
     *
     * @param p_element
     *     element to check
     * @return true if the element could have been inserted into the bloom filter (false positives possible)
     */
    public boolean contains(final Short p_element) {
        boolean ret = true;
        byte[] bytes;
        int[] hashes;

        bytes = new byte[2];
        bytes[0] = (byte) (p_element & 0xff);
        bytes[1] = (byte) (p_element >> 8 & 0xff);

        hashes = createHashes(bytes, m_k);
        for (int hash : hashes) {
            if (!m_bitset.get(Math.abs(hash % m_sizeOfBitset))) {
                ret = false;
                break;
            }
        }

        return ret;
    }

}
