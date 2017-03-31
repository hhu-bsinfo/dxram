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
 * Container interface for storing vertices for conversion. This allows
 * us to store vertices with non continuous IDs first before creating
 * a continuous list with vertex IDs
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.02.2016
 */
public interface VertexStorage {
    /**
     * Always return a vertex for the specified ID. The ID given does not to be a continuous ID and should NOT be the ID
     * assigned to the vertex. Make sure to generate continuous IDs on your own and assign these to the vertices. Make
     * sure these IDs are starting at 1
     * (not 0). Ensure that this function is implemented thread safe
     * @param p_hashValue
     *            Hash value used in the source representation. If no mapping exists for this so far, create one.
     * @return Continuous VertexSimple ID for the hash value which is used for the output graph.
     */
    long getVertexId(final long p_hashValue);

    /**
     * Add a neighbor relationship to a vertex.
     * @param p_vertexId
     *            Continuous vertex ID of the vertex to add the neighbor to.
     * @param p_neighbourVertexId
     *            Continous vertex ID of the neighbor to add.
     */
    void putNeighbour(final long p_vertexId, final long p_neighbourVertexId);

    long getNeighbours(final long p_vertexId, final long[] p_buffer);

    /**
     * Get the total number of vertices stored so far. This equals the highest continuous ID.
     * @return Total number of vertices.
     */
    long getTotalVertexCount();

    /**
     * Get the total edge count so far.
     * @return Total edge count.
     */
    long getTotalEdgeCount();

    /**
     * Get the (currently) total amount of memory the internally used data structures consume.
     * @return Total memory in bytes for the internal data structures.
     */
    long getTotalMemoryDataStructures();
}
