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

package de.hhu.bsinfo.dxgraph.data;

import java.util.Arrays;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * List of root vertex ids used as entry points for various graph algorithms.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class GraphRootList extends DataStructure {
    private long[] m_roots = new long[0];

    /**
     * Constructor
     */
    public GraphRootList() {
    }

    /**
     * Constructor
     *
     * @param p_id
     *         Chunk id to assign.
     */
    public GraphRootList(final long p_id) {
        super(p_id);
    }

    /**
     * Constructor
     *
     * @param p_id
     *         Chunk id to assign.
     * @param p_roots
     *         Initial root list to assign,
     */
    public GraphRootList(final long p_id, final long[] p_roots) {
        super(p_id);

        m_roots = p_roots;
    }

    /**
     * Constructor
     *
     * @param p_id
     *         Chunk id to assign.
     * @param p_numRoots
     *         Pre-allocate space for a number of roots.
     */
    public GraphRootList(final long p_id, final int p_numRoots) {
        super(p_id);

        m_roots = new long[p_numRoots];
    }

    /**
     * Get the list of roots.
     *
     * @return List of roots.
     */
    public long[] getRoots() {
        return m_roots;
    }

    /**
     * Resize the static allocated root list.
     *
     * @param p_count
     *         Number of roots to resize the list to.
     */
    public void setRootCount(final int p_count) {
        if (p_count != m_roots.length) {
            // grow or shrink array
            m_roots = Arrays.copyOf(m_roots, p_count);
        }
    }

    // -----------------------------------------------------------------------------

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLongArray(m_roots);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_roots = p_importer.readLongArray(m_roots);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofLongArray(m_roots);
    }

    @Override
    public String toString() {
        String str = "GraphRootList[m_id " + Long.toHexString(getID()) + ", numRoots " + m_roots.length + "]: ";
        int counter = 0;
        for (Long v : m_roots) {
            str += Long.toHexString(v) + ", ";
            counter++;
            // avoid long strings
            if (counter > 9) {
                break;
            }
        }

        return str;
    }
}
