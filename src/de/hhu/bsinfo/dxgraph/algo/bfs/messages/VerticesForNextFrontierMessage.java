package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Message to send non local vertices for BFS to the node owning them for processing.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 13.05.16
 */
public class VerticesForNextFrontierMessage extends AbstractMessage {

	private int m_numOfVertices;
	private int m_batchSize;
	private long[] m_vertexIDs;
	private int m_vertexPos;

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
	 * @param p_destination the destination
	 * @param p_batchSize   size of the buffer to store the vertex ids to send.
	 */
	public VerticesForNextFrontierMessage(final short p_destination, final int p_batchSize) {
		super(p_destination, BFSMessages.TYPE, BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE);

		m_batchSize = p_batchSize;
		m_vertexIDs = new long[p_batchSize];
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
	 * Check if this batch is full.
	 *
	 * @return True if full, false if batch size not reached, yet.
	 */
	public boolean isBatchFull() {
		return m_batchSize == m_numOfVertices;
	}

	public boolean addVertex(final long p_vertex) {
		if (m_vertexIDs.length == m_numOfVertices) {
			return false;
		}

		m_vertexIDs[m_vertexPos++] = p_vertex & 0xFFFFFFFFFFFFL;
		m_numOfVertices++;
		return true;
	}

	public long getVertex() {
		if (m_vertexIDs.length == m_vertexPos) {
			return -1;
		}

		return m_vertexIDs[m_vertexPos++];
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_numOfVertices);
		for (int i = 0; i < m_numOfVertices; i++) {
			p_buffer.putLong(m_vertexIDs[i]);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_numOfVertices = p_buffer.getInt();
		m_vertexIDs = new long[m_numOfVertices];
		for (int i = 0; i < m_numOfVertices; i++) {
			m_vertexIDs[i] = p_buffer.getLong();
		}
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES + m_numOfVertices * Long.BYTES;
	}
}
