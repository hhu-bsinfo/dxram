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

package de.hhu.bsinfo.dxgraph.conv;

/**
 * Interface for a data structure to buffer binary edges read from a binary edge list file.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.05.2016
 */
interface BinaryEdgeBuffer {

    /**
     * Add a binary edge to the buffer. Only a single thread is calling this.
     * @param p_val
     *            Source vertex of the edge.
     * @param p_val2
     *            Target vertex of the edge.
     * @return True if adding successful, false otherwise (buffer currently full).
     */
    boolean pushBack(final long p_val, final long p_val2);

    /**
     * Pop a single edge from the front of the buffer. Multiple threads are calling this.
     * @param p_retVals
     *            Long array of size 2 to store the edge to.
     * @return 0 if buffer empty, -1 on failure and 2 on success
     */
    int popFront(final long[] p_retVals);

    /**
     * Check if the buffer is empty.
     * @return True if empty, false otherwise.
     */
    boolean isEmpty();
}
