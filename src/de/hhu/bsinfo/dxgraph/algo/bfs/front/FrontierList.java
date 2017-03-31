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
 * Interface for a frontier list used on level synchronous
 * BFS.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 23.03.2016
 */
public interface FrontierList {

    /**
     * Push back a value/Add a value to the list.
     * @param p_val
     *            Value to add.
     * @return True if new value was pushed back, false if value already set
     */
    boolean pushBack(final long p_val);

    /**
     * Check if a certain value is available/set in the frontier list.
     * @param p_val
     *            Value to check.
     * @return True if this value is available in the list.
     */
    boolean contains(final long p_val);

    /**
     * Get the total capacity (max size) of the list.
     * @return Capacity of the list.
     */
    long capacity();

    /**
     * Get the number of elements in the list.
     * @return Number of elements in list.
     */
    long size();

    /**
     * Check if the list contains no elements.
     * @return True if empty.
     */
    boolean isEmpty();

    /**
     * Reset the list and clear its contents.
     */
    void reset();

    /**
     * Remove an element from the list.
     * @return Element removed or -1 if empty.
     */
    long popFront();
}
