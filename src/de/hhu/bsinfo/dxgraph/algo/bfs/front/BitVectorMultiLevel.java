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

package de.hhu.bsinfo.dxgraph.algo.bfs.front;

/**
 * Implementation of a frontier list. Extended version of
 * the normal BitVector adding another BitVector to
 * speed up element search.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public class BitVectorMultiLevel implements FrontierList {
    private long m_maxElementCount;
    private long[] m_vectorL0;
    private long[] m_vectorL1;

    private long m_itPos;

    private long m_count;

    /**
     * Constructor
     * @param p_maxElementCount
     *            Specify the maximum number of elements.
     */
    public BitVectorMultiLevel(final long p_maxElementCount) {
        m_maxElementCount = p_maxElementCount;
        m_vectorL0 = new long[(int) ((p_maxElementCount / 64L) + 1L)];
        m_vectorL1 = new long[(int) ((m_vectorL0.length / 64L) + 1)];
    }

    @Override
    public boolean pushBack(final long p_index) {
        long tmp = 1L << (p_index % 64L);
        int idx = (int) (p_index / 64L);
        if ((m_vectorL0[idx] & tmp) == 0) {
            m_count++;
            m_vectorL0[idx] |= tmp;
            m_vectorL1[idx / 64] |= 1L << (idx % 64L);
            return true;
        }

        return false;
    }

    @Override
    public boolean contains(final long p_val) {
        long tmp = 1L << (p_val % 64L);
        int idx = (int) (p_val / 64L);
        return (m_vectorL0[idx] & tmp) != 0;
    }

    @Override
    public long capacity() {
        return m_maxElementCount;
    }

    @Override
    public long size() {
        return m_count;
    }

    @Override
    public boolean isEmpty() {
        return m_count == 0;
    }

    @Override
    public void reset() {
        m_itPos = 0;
        m_count = 0;
        for (int i = 0; i < m_vectorL0.length; i++) {
            m_vectorL0[i] = 0;
        }
        for (int i = 0; i < m_vectorL1.length; i++) {
            m_vectorL1[i] = 0;
        }
    }

    @Override
    public long popFront() {
        while (m_count > 0) {
            final int posL1 = (int) (m_itPos / 4096L);
            if (m_vectorL1[posL1] != 0) {
                final int bitPosL1 = (int) ((m_itPos % 4096) / 64);
                for (int idxL1 = bitPosL1; idxL1 < 64; idxL1++) {
                    if ((m_vectorL1[posL1] & (1L << idxL1)) != 0) {
                        final int posL0 = (int) (m_itPos / 64L);
                        for (int idxL0 = (int) (m_itPos % 64); idxL0 < 64; idxL0++) {
                            if ((m_vectorL0[posL0] & (1L << idxL0)) != 0) {
                                long tmp = m_itPos;
                                m_itPos++;
                                m_count--;
                                return tmp;
                            } else {
                                m_itPos++;
                            }
                        }
                    } else {
                        m_itPos += 64L;
                    }
                }
            } else {
                m_itPos += 4096L;
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        return "[m_count " + m_count + ", m_itPos " + m_itPos + "]";
    }
}
