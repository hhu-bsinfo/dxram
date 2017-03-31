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

import java.util.TreeSet;

/**
 * Frontier implementation using Java's TreeSet.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public class TreeSetFifo implements FrontierList {

    private long m_maxElementCount;
    private TreeSet<Long> m_tree = new TreeSet<>();

    public TreeSetFifo(final long p_maxElementCount) {
        m_maxElementCount = p_maxElementCount;
    }

    @Override
    public boolean pushBack(final long p_val) {
        return m_tree.add(p_val);
    }

    @Override
    public boolean contains(final long p_val) {
        return m_tree.contains(p_val);
    }

    @Override
    public long capacity() {
        return m_maxElementCount;
    }

    @Override
    public long size() {
        return m_tree.size();
    }

    @Override
    public boolean isEmpty() {
        return m_tree.isEmpty();
    }

    @Override
    public void reset() {
        m_tree.clear();
    }

    @Override
    public long popFront() {
        Long tmp = m_tree.pollFirst();
        if (tmp == null) {
            return -1;
        }
        return tmp;
    }

}
