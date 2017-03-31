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

package de.hhu.bsinfo.dxgraph.load.oel;

import de.hhu.bsinfo.dxgraph.data.VertexSimple;

/**
 * Interface for an ordered edge list providing vertices.
 * This can be implemented by a file reading with buffering backend.
 * VertexSimple indices have to start with id 1
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public interface OrderedEdgeList {

    /**
     * Read vertex data. This does not re-base the vertex id or any ids of the neighbors.
     * @return VertexSimple read or null if no vertices are left to read.
     */
    VertexSimple readVertex();
}
