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

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Basic vertex object that can be extended with further data if desired.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.09.2016
 */
public class Vertex extends DataStructure {
    public static final long INVALID_ID = ChunkID.INVALID_ID;

    private boolean m_neighborsAreEdgeObjects;
    private boolean m_locked;

    private long[] m_neighborIDs = new long[0];

    /**
     * Constructor
     */
    public Vertex() {
    }

    /**
     * Constructor
     *
     * @param p_id
     *         Chunk id to assign.
     */
    public Vertex(final long p_id) {
        super(p_id);
    }

    // -----------------------------------------------------------------------------

    /**
     * Check if the neighbor IDs of this vertex refer to actual edge objects
     * that can store data.
     *
     * @return If true, neighbor IDs refer to actual edge objects, false if
     * they refer to the neighbor vertex directly.
     */
    public boolean areNeighborsEdgeObjects() {
        return m_neighborsAreEdgeObjects;
    }

    /**
     * Set if the neighbor IDs refer to edge objects or directly to the
     * neighbor vertices.
     *
     * @param p_edgeObjects
     *         True if refering to edge objects, false to vertex objects.
     */
    public void setNeighborsAreEdgeObjects(final boolean p_edgeObjects) {
        m_neighborsAreEdgeObjects = p_edgeObjects;
    }

    /**
     * Check if this vertex was locked.
     *
     * @return True if locked, false otherwise.
     */
    public boolean isLocked() {
        return m_locked;
    }

    /**
     * Set this vertex locked.
     *
     * @param p_locked
     *         True for locked, false unlocked.
     */
    public void setLocked(boolean p_locked) {
        m_locked = p_locked;
    }

    /**
     * Add a new neighbour to the currently existing list.
     * This will expand the array by one entry and
     * add the new neighbour at the end.
     *
     * @param p_neighbour
     *         Neighbour vertex Id to add.
     */
    public void addNeighbour(final long p_neighbour) {
        setNeighbourCount(m_neighborIDs.length + 1);
        m_neighborIDs[m_neighborIDs.length - 1] = p_neighbour;
    }

    /**
     * Get the neighbour array.
     *
     * @return Neighbour array with vertex ids.
     */
    public long[] getNeighbours() {
        return m_neighborIDs;
    }

    /**
     * Get the number of neighbors of this vertex.
     *
     * @return Number of neighbors.
     */
    public int getNeighborCount() {
        return m_neighborIDs.length;
    }

    /**
     * Resize the neighbour array.
     *
     * @param p_count
     *         Number of neighbours to resize to.
     */
    public void setNeighbourCount(final int p_count) {
        if (p_count != m_neighborIDs.length) {
            // grow or shrink array
            m_neighborIDs = Arrays.copyOf(m_neighborIDs, p_count);
        }
    }

    // -----------------------------------------------------------------------------

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLongArray(m_neighborIDs);

        byte flags = 0;
        flags |= m_neighborsAreEdgeObjects ? 1 : 0;
        flags |= m_locked ? 1 << 1 : 0;
        p_exporter.writeByte(flags);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_neighborIDs = p_importer.readLongArray(m_neighborIDs);

        byte flags = p_importer.readByte((byte) 0);
        m_neighborsAreEdgeObjects = (flags & 1 << 1) > 0;
        m_locked = (flags & 1 << 2) > 0;
    }

    @Override
    public int sizeofObject() {
        int size = 0;

        size += Byte.BYTES;
        size += ObjectSizeUtil.sizeofLongArray(m_neighborIDs);
        return size;
    }

    @Override
    public String toString() {
        return "Vertex[m_id " + Long.toHexString(getID()) + ", m_neighborsAreEdgeObjects " + m_neighborsAreEdgeObjects + ", m_locked " + m_locked +
                ", m_neighborsCount " + m_neighborIDs.length + "]: ";
    }
}
