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

import java.util.Map;
import java.util.TreeMap;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Graph partition index for partitioned graph.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.04.2016
 */
public class GraphPartitionIndex extends DataStructure {
    private Map<Integer, Entry> m_index = new TreeMap<>();
    private int m_size;

    /**
     * Constructor
     */
    public GraphPartitionIndex() {
    }

    /**
     * Calculate the total vertex count based on the partition index (i.e. summing up vertex counts of all partitions).
     *
     * @return Total vertex count of the full graph.
     */
    public long calcTotalVertexCount() {
        long total = 0;
        for (Map.Entry<Integer, Entry> entry : m_index.entrySet()) {
            total += entry.getValue().getVertexCount();
        }
        return total;
    }

    /**
     * Calculate the total edge count based on the partition index (i.e. summing up edge counts of all partitions).
     *
     * @return Total edge count of the full graph.
     */
    public long calcTotalEdgeCount() {
        long total = 0;
        for (Map.Entry<Integer, Entry> entry : m_index.entrySet()) {
            total += entry.getValue().getEdgeCount();
        }
        return total;
    }

    /**
     * Set a partition entry for the index.
     *
     * @param p_entry
     *         Entry to set/add.
     */
    public void setPartitionEntry(final Entry p_entry) {
        m_index.put(p_entry.m_partitionIndex, p_entry);
    }

    /**
     * Get a partition index entry from the index.
     *
     * @param p_partitionId
     *         Id of the partition index entry to get.
     * @return Partition index entry or null if there is no entry for the specified id.
     */
    public Entry getPartitionIndex(final int p_partitionId) {
        return m_index.get(p_partitionId);
    }

    /**
     * Get the total number of partitions
     *
     * @return Total number of partitions.
     */
    public int getTotalPartitionCount() {
        return m_index.size();
    }

    /**
     * Rebase a graph global vertexId to a partition local vertex id using the index.
     *
     * @param p_vertexId
     *         Graph global vertexId to rebase.
     * @return Rebased vertex id to the partition the vertex is in.
     */
    public long rebaseGlobalVertexIdToLocalPartitionVertexId(final long p_vertexId) {
        // find section the vertex (of the neighbor) is in
        long globalVertexIDOffset = 0;
        for (Entry entry : m_index.values()) {
            if (p_vertexId >= globalVertexIDOffset && p_vertexId < globalVertexIDOffset + entry.m_vertexCount) {
                return ChunkID.getChunkID(entry.m_nodeId, p_vertexId - globalVertexIDOffset) + 1;
            }

            globalVertexIDOffset += entry.m_vertexCount;
        }

        // out of range ID
        return ChunkID.INVALID_ID;
    }

    /**
     * Rebase multiple graph global vertexIds in plance to partition local vertex ids using the index.
     *
     * @param p_vertexIds
     *         Graph global vertexIds to rebase.
     * @return True if rebasing all IDs was successful, false if one or multiple could not be rebased, out of range
     */
    public boolean rebaseGlobalVertexIdToLocalPartitionVertexId(final long[] p_vertexIds) {
        boolean res = true;

        // utilize locality instead of calling function
        for (int i = 0; i < p_vertexIds.length; i++) {
            // out of range ID, default assign if not found in loop
            long tmp = ChunkID.INVALID_ID;

            // find section the vertex (of the neighbor) is in
            long globalVertexIDOffset = 0;
            for (Entry entry : m_index.values()) {
                if (p_vertexIds[i] >= globalVertexIDOffset && p_vertexIds[i] < globalVertexIDOffset + entry.m_vertexCount) {
                    tmp = ChunkID.getChunkID(entry.m_nodeId, p_vertexIds[i] - globalVertexIDOffset) + 1;
                    break;
                }

                globalVertexIDOffset += entry.m_vertexCount;
            }

            if (tmp == ChunkID.INVALID_ID) {
                res = false;
            }

            p_vertexIds[i] = tmp;
        }

        return res;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_index.size());
        for (Entry entry : m_index.values()) {
            p_exporter.exportObject(entry);
        }
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_size = p_importer.readInt(m_size);
        for (int i = 0; i < m_size; i++) {
            Entry entry = new Entry();
            p_importer.importObject(entry);
            if (m_index.size() == i) {
                m_index.put(entry.m_partitionIndex, entry);
            }
        }
    }

    @Override
    public int sizeofObject() {
        if (m_index.isEmpty()) {
            return Integer.BYTES;
        } else {
            return Integer.BYTES + m_index.size() * m_index.get(0).sizeofObject();
        }
    }

    @Override
    public String toString() {
        String str = "";
        for (Entry entry : m_index.values()) {
            str += entry + "\n";
        }

        return str;
    }

    /**
     * Single partition index entry.
     *
     * @author Stefan Nothaas, stefan.nothaas@hhu.de, 21.04.2016
     */
    public static class Entry extends DataStructure {
        private short m_nodeId = -1;
        private int m_partitionIndex = -1;
        private long m_vertexCount = -1;
        private long m_edgeCount = -1;
        private long m_fileStartOffset = -1;

        /**
         * Default constructor
         */
        public Entry() {

        }

        /**
         * Constructor
         *
         * @param p_nodeId
         *         Node id the partition gets assigned to.
         * @param p_partitionIndex
         *         Partition index.
         * @param p_vertexCount
         *         Number of vertices in this partition.
         * @param p_edgeCount
         *         Number of edges in this partition.
         * @param p_fileStartOffset
         *         Offset in the file where the partition starts
         */
        public Entry(final short p_nodeId, final int p_partitionIndex, final long p_vertexCount, final long p_edgeCount, final long p_fileStartOffset) {
            m_nodeId = p_nodeId;
            m_partitionIndex = p_partitionIndex;
            m_vertexCount = p_vertexCount;
            m_edgeCount = p_edgeCount;
            m_fileStartOffset = p_fileStartOffset;
        }

        /**
         * Get the node id this partition is assigned to.
         *
         * @return Node Id.
         */
        public short getNodeId() {
            return m_nodeId;
        }

        /**
         * Get the partition id.
         *
         * @return Partition id.
         */
        public int getPartitionId() {
            return m_partitionIndex;
        }

        /**
         * Get the vertex count of the partition.
         *
         * @return VertexSimple count.
         */
        public long getVertexCount() {
            return m_vertexCount;
        }

        /**
         * Get the edge count of the partition.
         *
         * @return Edge count.
         */
        public long getEdgeCount() {
            return m_edgeCount;
        }

        /**
         * Offset in the file where the partition starts.
         *
         * @return File offset.
         */
        public long getFileStartOffset() {
            return m_fileStartOffset;
        }

        @Override
        public void exportObject(final Exporter p_exporter) {
            p_exporter.writeShort(m_nodeId);
            p_exporter.writeInt(m_partitionIndex);
            p_exporter.writeLong(m_vertexCount);
            p_exporter.writeLong(m_edgeCount);
            p_exporter.writeLong(m_fileStartOffset);
        }

        @Override
        public void importObject(final Importer p_importer) {
            m_nodeId = p_importer.readShort(m_nodeId);
            m_partitionIndex = p_importer.readInt(m_partitionIndex);
            m_vertexCount = p_importer.readLong(m_vertexCount);
            m_edgeCount = p_importer.readLong(m_edgeCount);
            m_fileStartOffset = p_importer.readLong(m_fileStartOffset);
        }

        @Override
        public int sizeofObject() {
            return Short.BYTES + Integer.BYTES + Long.BYTES + Long.BYTES + Long.BYTES;
        }

        @Override
        public String toString() {
            return m_partitionIndex + ", " + NodeID.toHexString(m_nodeId) + ", " + m_vertexCount + ", " + m_edgeCount + ", " + m_fileStartOffset;
        }
    }
}
