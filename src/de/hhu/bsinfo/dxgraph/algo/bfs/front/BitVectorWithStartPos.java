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
 * Implementation of a frontier list. Extended version
 * of a normal BitVector keeping the first value
 * within the vector cached to speed up element search.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public class BitVectorWithStartPos implements FrontierList {
    private long m_maxElementCount;
    private long[] m_vector;

    private long m_itPos;
    private long m_count;
    private long m_firstValuePos = -1;

    /**
     * Constructor
     * @param p_vertexCount
     *            Total number of vertices.
     */
    public BitVectorWithStartPos(final long p_vertexCount) {
        m_maxElementCount = p_vertexCount;
        m_vector = new long[(int) ((p_vertexCount / 64L) + 1L)];
    }

    @Override
    public boolean pushBack(final long p_index) {
        long tmp = 1L << (p_index % 64L);
        int idx = (int) (p_index / 64L);
        if ((m_vector[idx] & tmp) == 0) {
            m_count++;
            m_vector[idx] |= tmp;
            if (m_firstValuePos > p_index) {
                m_firstValuePos = p_index;
            }

            return true;
        }

        return false;
    }

    @Override
    public boolean contains(final long p_val) {
        long tmp = 1L << (p_val % 64L);
        int idx = (int) (p_val / 64L);
        return (m_vector[idx] & tmp) != 0;
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
        m_firstValuePos = m_vector.length * 64L;
        for (int i = 0; i < m_vector.length; i++) {
            m_vector[i] = 0;
        }
    }

    @Override
    public long popFront() {
        while (m_count > 0) {
            // speed things up for first value, jump
            if (m_firstValuePos > m_itPos) {
                m_itPos = m_firstValuePos;
            }

            if ((m_vector[(int) (m_itPos / 64L)] & (1L << m_itPos % 64L)) != 0) {
                long tmp = m_itPos;
                m_itPos++;
                m_count--;
                return tmp;
            }

            m_itPos++;
        }

        return -1;
    }

    @Override
    public String toString() {
        return "[m_count " + m_count + ", m_itPos " + m_itPos + "]";
    }
}
