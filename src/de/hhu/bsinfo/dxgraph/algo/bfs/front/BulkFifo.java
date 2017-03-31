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
 * Extending the naive implementation, this adds a check
 * if the element is already stored to avoid extreme memory footprint
 * when inserting identical values multiple times.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public class BulkFifo extends BulkFifoNaive {

    /**
     * Constructor
     */
    public BulkFifo() {
        super();
    }

    /**
     * Constructor
     * @param p_bulkSize
     *            Specify the bulk size for block allocation.
     */
    public BulkFifo(final int p_bulkSize) {
        super(p_bulkSize);
    }

    @Override
    public boolean pushBack(final long p_val) {
        if (contains(p_val)) {
            return false;
        }

        super.pushBack(p_val);
        return true;
    }
}
