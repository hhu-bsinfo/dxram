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

import java.util.Arrays;

/**
 * Implementation of a frontier list based on bulk allocated arrays.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public class BulkFifoNaive implements FrontierList {
    protected int m_bulkSize = 16 * 1024 * 1024 / Long.BYTES;
    protected static final int MS_BULK_BLOCK_GROWTH = 10;
    protected long[][] m_chainedFifo = new long[MS_BULK_BLOCK_GROWTH][];

    protected int m_posBack;
    protected int m_blockBack;

    protected int m_posFront;
    protected int m_blockFront;

    /**
     * Constructor
     * Default bulk size is 16MB.
     */
    public BulkFifoNaive() {
        m_chainedFifo[0] = new long[m_bulkSize];
    }

    /**
     * Constructor
     * @param p_bulkSize
     *            Specify the bulk size for block allocation.
     */
    public BulkFifoNaive(final int p_bulkSize) {
        m_bulkSize = p_bulkSize;
        m_chainedFifo[0] = new long[m_bulkSize];
    }

    @Override
    public boolean pushBack(final long p_val) {
        if (m_posBack == m_bulkSize) {
            // grow back
            // check if next bulk block exists
            if (++m_blockBack >= m_chainedFifo.length) {
                // grow bulk block
                m_chainedFifo = Arrays.copyOf(m_chainedFifo, m_chainedFifo.length + MS_BULK_BLOCK_GROWTH);
            }

            m_chainedFifo[m_blockBack] = new long[m_bulkSize];
            m_posBack = 0;
        }

        if (m_posBack < m_bulkSize) {
            m_chainedFifo[m_blockBack][m_posBack++] = p_val;
            return true;
        }

        return false;
    }

    @Override
    public boolean contains(final long p_val) {
        int curBlock = m_blockFront;
        int curPos = m_posFront;

        do {
            int posEnd = m_bulkSize;
            if (curBlock == m_blockBack) {
                posEnd = m_posBack;
            }

            for (int i = curPos; i < posEnd; i++) {
                if (m_chainedFifo[curBlock][i] == p_val) {
                    return true;
                }
            }

            curBlock++;
            curPos = 0;
        } while (curBlock < m_blockBack);

        return false;
    }

    @Override
    public long capacity() {
        return m_bulkSize;
    }

    @Override
    public long size() {
        if (m_blockFront == m_blockBack) {
            return m_posBack - m_posFront;
        } else {
            int size = 0;
            size += m_bulkSize - m_posFront;
            size += m_bulkSize * (m_blockBack - m_blockFront + 1);
            size += m_posBack;
            return size;
        }
    }

    @Override
    public boolean isEmpty() {
        return m_blockBack == m_blockFront && m_posBack == m_posFront;
    }

    @Override
    public void reset() {
        m_posBack = 0;
        m_blockBack = 0;

        m_posFront = 0;
        m_blockFront = 0;
    }

    @Override
    public long popFront() {
        if (m_blockBack == m_blockFront && m_posBack == m_posFront) {
            return -1;
        }

        long tmp = m_chainedFifo[m_blockFront][m_posFront++];
        // go to next block, jump if necessary
        if (m_posFront == m_bulkSize) {
            m_blockFront++;
            m_posFront = 0;
        }

        return tmp;
    }
}
