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

package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import de.hhu.bsinfo.dxgraph.DXGraphMessageTypes;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Message to send non local vertices for BFS to the node owning them for processing.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.05.2016
 */
public class VerticesForNextFrontierMessage extends Message {

    private int m_numOfVertices;
    private int m_batchSize;
    private long[] m_vertexIDs;
    private int m_vertexPos;

    private int m_numOfNeighbors;
    private long[] m_neighborIDs;
    private int m_neighborPos;

    /**
     * Creates an instance of VerticesForNextFrontierRequest.
     * This constructor is used when receiving this message.
     */
    public VerticesForNextFrontierMessage() {
        super();
    }

    /**
     * Creates an instance of VerticesForNextFrontierRequest
     *
     * @param p_destination
     *         the destination
     * @param p_batchSize
     *         size of the buffer to store the vertex ids to send.
     */
    public VerticesForNextFrontierMessage(final short p_destination, final int p_batchSize) {
        super(p_destination, DXGraphMessageTypes.BFS_MESSAGES_TYPE, BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE);

        m_batchSize = p_batchSize;
        m_vertexIDs = new long[p_batchSize];
        m_neighborIDs = new long[p_batchSize];
    }

    /**
     * Reset the buffers and position to re-use the message (lowering memory footprint)
     */
    public void reset() {
        m_numOfVertices = 0;
        m_vertexPos = 0;
        m_numOfNeighbors = 0;
        m_neighborPos = 0;
    }

    /**
     * Get max size of this vertex batch.
     *
     * @return Max number of vertices possible for this batch.
     */
    public int getBatchSize() {
        return m_batchSize;
    }

    /**
     * Get the actual number of vertices in this batch.
     *
     * @return Number of vertices in this batch.
     */
    public int getNumVerticesInBatch() {
        return m_numOfVertices;
    }

    /**
     * Determine if any neighbors were sent with this message.
     * If neighbors are included, this message is sent from a node
     * running bottom up mode, thus needing different treatment than
     * a message with vertices only (top down mode).
     *
     * @return Number of neighbors in batch
     */
    public int getNumNeighborsInBatch() {
        return m_numOfNeighbors;
    }

    /**
     * Add a vertex to the batch
     *
     * @param p_vertex
     *         VertexSimple to add
     * @return True if adding successful, false if batch is full
     */
    public boolean addVertex(final long p_vertex) {
        if (m_vertexIDs.length == m_numOfVertices) {
            return false;
        }

        m_vertexIDs[m_vertexPos++] = p_vertex;
        m_numOfVertices++;
        return true;
    }

    /**
     * Add a neighbor to the batch (optional to vertices)
     *
     * @param p_neighbor
     *         Neighbor to add
     * @return True if successful, false if batch is full
     */
    public boolean addNeighbor(final long p_neighbor) {
        if (m_neighborIDs.length == m_numOfNeighbors) {
            return false;
        }

        m_neighborIDs[m_neighborPos++] = p_neighbor;
        m_numOfNeighbors++;
        return true;
    }

    /**
     * Get the next vertex in the batch. An internal counter is incremented.
     *
     * @return Valid vertex id if successful, -1 if batch is empty.
     */
    public long getVertex() {
        if (m_vertexIDs.length == m_vertexPos) {
            return -1;
        }

        return m_vertexIDs[m_vertexPos++];
    }

    /**
     * Get the next neighbor in the batch. An internal counter is incremented.
     *
     * @return Valid neighbor id if successful, -1 if batch is empty
     */
    public long getNeighbor() {
        if (m_neighborIDs.length == m_neighborPos) {
            return -1;
        }

        return m_neighborIDs[m_neighborPos++];
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_numOfVertices);
        p_exporter.writeLongArray(m_vertexIDs);
        p_exporter.writeInt(m_numOfNeighbors);
        p_exporter.writeLongArray(m_neighborIDs);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_numOfVertices = p_importer.readInt(m_numOfVertices);
        m_vertexIDs = p_importer.readLongArray(m_vertexIDs);
        m_numOfNeighbors = p_importer.readInt(m_numOfNeighbors);
        m_neighborIDs = p_importer.readLongArray(m_neighborIDs);
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + ObjectSizeUtil.sizeofLongArray(m_vertexIDs) + Integer.BYTES + ObjectSizeUtil.sizeofLongArray(m_neighborIDs);
    }
}
