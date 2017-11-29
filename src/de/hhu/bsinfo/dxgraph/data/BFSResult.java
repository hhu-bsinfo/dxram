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

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Data structure holding results of a single BFS run.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 20.05.2016
 */
public class BFSResult extends DataStructure {

    public long m_rootVertexId = ChunkID.INVALID_ID;
    public long m_graphFullSizeVertices = 0;
    public long m_graphFullSizeEdges = 0;
    public long m_graphPartitionSizeVertices = 0;
    public long m_graphPartitionSizeEdges = 0;
    public long m_totalVisitedVertices = 0;
    public long m_totalVisitedEdges = 0;
    public long m_totalVerticesTraversed = 0;
    public long m_totalEdgesTraversed = 0;
    public long m_maxTraversedVertsPerSecond = 0;
    public long m_maxTraversedEdgesPerSecond = 0;
    public long m_avgTraversedVertsPerSecond = 0;
    public long m_avgTraversedEdgesPerSecond = 0;
    public long m_totalTimeMs = 0;
    public int m_totalBFSDepth = 0;

    public BFSResult() {

    }

    @Override
    public void importObject(final Importer p_importer) {
        m_rootVertexId = p_importer.readLong(m_rootVertexId);
        m_graphFullSizeVertices = p_importer.readLong(m_graphFullSizeVertices);
        m_graphFullSizeEdges = p_importer.readLong(m_graphFullSizeEdges);
        m_graphPartitionSizeVertices = p_importer.readLong(m_graphPartitionSizeVertices);
        m_graphPartitionSizeEdges = p_importer.readLong(m_graphPartitionSizeEdges);
        m_totalVisitedVertices = p_importer.readLong(m_totalVisitedVertices);
        m_totalVisitedEdges = p_importer.readLong(m_totalVisitedEdges);
        m_totalVerticesTraversed = p_importer.readLong(m_totalVerticesTraversed);
        m_totalEdgesTraversed = p_importer.readLong(m_totalEdgesTraversed);
        m_maxTraversedVertsPerSecond = p_importer.readLong(m_maxTraversedVertsPerSecond);
        m_maxTraversedEdgesPerSecond = p_importer.readLong(m_maxTraversedEdgesPerSecond);
        m_avgTraversedVertsPerSecond = p_importer.readLong(m_avgTraversedVertsPerSecond);
        m_avgTraversedEdgesPerSecond = p_importer.readLong(m_avgTraversedEdgesPerSecond);
        m_totalTimeMs = p_importer.readLong(m_totalTimeMs);
        m_totalBFSDepth = p_importer.readInt(m_totalBFSDepth);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES * 14 + Integer.BYTES;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeLong(m_rootVertexId);
        p_exporter.writeLong(m_graphFullSizeVertices);
        p_exporter.writeLong(m_graphFullSizeEdges);
        p_exporter.writeLong(m_graphPartitionSizeVertices);
        p_exporter.writeLong(m_graphPartitionSizeEdges);
        p_exporter.writeLong(m_totalVisitedVertices);
        p_exporter.writeLong(m_totalVisitedEdges);
        p_exporter.writeLong(m_totalVerticesTraversed);
        p_exporter.writeLong(m_totalEdgesTraversed);
        p_exporter.writeLong(m_maxTraversedVertsPerSecond);
        p_exporter.writeLong(m_maxTraversedEdgesPerSecond);
        p_exporter.writeLong(m_avgTraversedVertsPerSecond);
        p_exporter.writeLong(m_avgTraversedEdgesPerSecond);
        p_exporter.writeLong(m_totalTimeMs);
        p_exporter.writeInt(m_totalBFSDepth);
    }

    @Override
    public String toString() {
        return "BFSResult " + ChunkID.toHexString(getID()) + ":\n" + "m_rootVertexId " + ChunkID.toHexString(m_rootVertexId) + '\n' +
                "m_graphFullSizeVertices " + m_graphFullSizeVertices + '\n' + "m_graphFullSizeEdges " + m_graphFullSizeEdges + '\n' +
                "m_graphPartitionSizeVertices " + m_graphPartitionSizeVertices + '\n' + "m_graphPartitionSizeEdges " + m_graphPartitionSizeEdges + '\n' +
                "m_totalVisitedVertices " + m_totalVisitedVertices + '\n' + "m_totalVisitedEdges " + m_totalVisitedEdges + '\n' + "m_totalVerticesTraversed " +
                m_totalVerticesTraversed + '\n' + "m_totalEdgesTraversed " + m_totalEdgesTraversed + '\n' + "m_maxTraversedVertsPerSecond " +
                m_maxTraversedVertsPerSecond + '\n' + "m_maxTraversedEdgesPerSecond " + m_maxTraversedEdgesPerSecond + '\n' + "m_avgTraversedVertsPerSecond " +
                m_avgTraversedVertsPerSecond + '\n' + "m_avgTraversedEdgesPerSecond " + m_avgTraversedEdgesPerSecond + '\n' + "m_totalTimeMs " + m_totalTimeMs +
                '\n' + "m_totalBFSDepth " + m_totalBFSDepth;
    }
}
