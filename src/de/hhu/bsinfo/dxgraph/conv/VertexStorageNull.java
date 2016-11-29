/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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
 * Dummy storage implementation for testing other parts of the converter.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 10.05.2016
 */
public class VertexStorageNull implements VertexStorage {

    @Override
    public long getVertexId(final long p_hashValue) {
        return p_hashValue;
    }

    @Override
    public void putNeighbour(final long p_vertexId, final long p_neighbourVertexId) {
    }

    @Override
    public long getNeighbours(final long p_vertexId, final long[] p_buffer) {
        return 0;
    }

    @Override
    public long getTotalVertexCount() {
        return 0;
    }

    @Override
    public long getTotalEdgeCount() {
        return 0;
    }

    @Override
    public long getTotalMemoryDataStructures() {
        return 0;
    }
}
