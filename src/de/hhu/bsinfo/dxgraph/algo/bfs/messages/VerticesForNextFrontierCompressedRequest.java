
package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import java.nio.ByteBuffer;

/**
 * Message to send non local vertices for BFS to the node owning them for processing.
 * Vertex IDs are "compressed" (using the local ID, only) due to pre-filtering by target node.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 13.05.16
 */
public class VerticesForNextFrontierCompressedRequest extends AbstractVerticesForNextFrontierRequest {

	private long[] m_compressedVertexIds;
	private int m_vertexIdsPos;
	private int m_vertexPos;

	/**
	 * Creates an instance of VerticesForNextFrontierCompressedRequest.
	 * This constructor is used when receiving this message.
	 */
	public VerticesForNextFrontierCompressedRequest() {
		super();
	}

	/**
	 * Creates an instance of VerticesForNextFrontierCompressedRequest
	 *
	 * @param p_destination the destination
	 * @param p_batchSize   size of the buffer to store the vertex ids to send.
	 */
	public VerticesForNextFrontierCompressedRequest(final short p_destination, final int p_batchSize) {
		super(p_destination, BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_COMPRESSED_REQUEST, p_batchSize);

		m_compressedVertexIds =
				new long[p_batchSize * 6 / Long.BYTES + (((p_batchSize * 6) % Long.BYTES) != 0 ? 1 : 0)];
	}

	@Override
	public boolean addVertex(final long p_vertex) {
		if (m_batchSize == m_numOfVertices) {
			return false;
		}

		long vertex = p_vertex & 0xFFFFFFFFFFFFL;
		switch (m_numOfVertices % 4) {
			case 0:
				m_compressedVertexIds[m_vertexIdsPos] = vertex << 12L;
				break;
			case 1:
				m_compressedVertexIds[m_vertexIdsPos++] |= (vertex >> 32L) & 0xFFFFL;
				m_compressedVertexIds[m_vertexIdsPos] = (vertex & 0xFFFFFFFFL) << 32L;
				break;
			case 2:
				m_compressedVertexIds[m_vertexIdsPos++] |= (vertex >> 16L) & 0xFFFFFFFFL;
				m_compressedVertexIds[m_vertexIdsPos] = (vertex & 0xFFFFL) << 48L;
				break;
			case 3:
				m_compressedVertexIds[m_vertexIdsPos++] |= vertex;
				break;
		}

		m_numOfVertices++;
		return true;
	}

	@Override
	public long getVertex() {
		long vertex = -1;

		if (m_vertexPos == m_numOfVertices) {
			return vertex;
		}

		switch (m_vertexPos % 4) {
			case 0:
				vertex = m_compressedVertexIds[m_vertexIdsPos] >> 12L;
				break;
			case 1:
				vertex = (m_compressedVertexIds[m_vertexIdsPos++] & 0xFFFFL) << 32L;
				vertex |= m_compressedVertexIds[m_vertexIdsPos] >> 32L;
				break;
			case 2:
				vertex = m_compressedVertexIds[m_vertexIdsPos++] & 0xFFFFFFFFL;
				vertex |= m_compressedVertexIds[m_vertexIdsPos] >> 48L;
				break;
			case 3:
				vertex = m_compressedVertexIds[m_vertexIdsPos++] & 0xFFFFFFFFFFFFL;
				break;
		}

		m_vertexPos++;

		return vertex;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_numOfVertices);
		p_buffer.putInt(m_compressedVertexIds.length);
		for (int i = 0; i < m_compressedVertexIds.length; i++) {
			p_buffer.putLong(m_compressedVertexIds[i]);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_numOfVertices = p_buffer.getInt();
		m_compressedVertexIds = new long[p_buffer.getInt()];
		for (int i = 0; i < m_compressedVertexIds.length; i++) {
			m_compressedVertexIds[i] = p_buffer.getLong();
		}
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES * 2 + m_compressedVertexIds.length * Long.BYTES;
	}
}
