
package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import java.nio.ByteBuffer;

/**
 * Message to send non local vertices for BFS to the node owning them for processing.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 13.05.16
 */
public class VerticesForNextFrontierRequest extends AbstractVerticesForNextFrontierRequest {

	private long[] m_vertexIDs;
	private int m_vertexPos;

	/**
	 * Creates an instance of VerticesForNextFrontierRequest.
	 * This constructor is used when receiving this message.
	 */
	public VerticesForNextFrontierRequest() {
		super();
	}

	/**
	 * Creates an instance of VerticesForNextFrontierRequest
	 *
	 * @param p_destination the destination
	 * @param p_batchSize   size of the buffer to store the vertex ids to send.
	 */
	public VerticesForNextFrontierRequest(final short p_destination, final int p_batchSize) {
		super(p_destination, BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_REQUEST, p_batchSize);

		m_vertexIDs = new long[p_batchSize];
	}

	@Override
	public boolean addVertex(final long p_vertex) {
		if (m_vertexIDs.length == m_numOfVertices) {
			return false;
		}

		m_vertexIDs[m_vertexPos++] = p_vertex & 0xFFFFFFFFFFFFL;
		m_numOfVertices++;
		return true;
	}

	@Override
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
