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

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxgraph.DXGraphMessageTypes;
import de.hhu.bsinfo.ethnet.core.AbstractMessage;

/**
 * Message to send non local vertices for BFS to the node owning them for processing.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 13.05.2016
 */
public class VerticesForNextFrontierMessage extends AbstractMessage {

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
     * @param p_destination
     *            the destination
     * @param p_batchSize
     *            size of the buffer to store the vertex ids to send.
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
     * @return Max number of vertices possible for this batch.
     */
    public int getBatchSize() {
        return m_batchSize;
    }

    /**
     * Get the actual number of vertices in this batch.
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
     * @return Number of neighbors in batch
     */
    public int getNumNeighborsInBatch() {
        return m_numOfNeighbors;
    }

    /**
     * Add a vertex to the batch
     * @param p_vertex
     *            VertexSimple to add
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
     * @param p_neighbor
     *            Neighbor to add
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
     * @return Valid neighbor id if successful, -1 if batch is empty
     */
    public long getNeighbor() {
        if (m_neighborIDs.length == m_neighborPos) {
            return -1;
        }

        return m_neighborIDs[m_neighborPos++];
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_numOfVertices);
        for (int i = 0; i < m_numOfVertices; i++) {
            p_buffer.putLong(m_vertexIDs[i]);
        }
        p_buffer.putInt(m_numOfNeighbors);
        for (int i = 0; i < m_numOfNeighbors; i++) {
            p_buffer.putLong(m_neighborIDs[i]);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_numOfVertices = p_buffer.getInt();
        m_vertexIDs = new long[m_numOfVertices];
        for (int i = 0; i < m_numOfVertices; i++) {
            m_vertexIDs[i] = p_buffer.getLong();
        }
        m_numOfNeighbors = p_buffer.getInt();
        m_neighborIDs = new long[m_numOfNeighbors];
        for (int i = 0; i < m_numOfNeighbors; i++) {
            m_neighborIDs[i] = p_buffer.getLong();
        }
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + m_numOfVertices * Long.BYTES + Integer.BYTES + m_numOfNeighbors * Long.BYTES;
    }
}
