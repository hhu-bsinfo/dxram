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

package de.hhu.bsinfo.dxgraph;

/**
 * List of task payloads in the dxgraph package.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public final class GraphTaskPayloads {
    public static final short TYPE = 1;
    public static final short SUBTYPE_GRAPH_LOAD_PART_INDEX = 0;
    public static final short SUBTYPE_GRAPH_LOAD_OEL = 1;
    public static final short SUBTYPE_GRAPH_LOAD_BFS_ROOTS = 2;
    public static final short SUBTYPE_GRAPH_ALGO_BFS = 3;

    /**
     * Static class
     */
    private GraphTaskPayloads() {
    }
}
