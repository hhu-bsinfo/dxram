
package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.menet.AbstractMessage;

public class VerticesForNextFrontierMessage extends AbstractMessage {

	private int m_numOfVertices;
	private long[] m_vertexIDs;

	/**
	 * Creates an instance of VerticesForNextFrontierMessage.
	 * This constructor is used when receiving this message.
	 */
	public VerticesForNextFrontierMessage() {
		super();
	}

	/**
	 * Creates an instance of VerticesForNextFrontierMessage
	 * @param p_destination
	 *            the destination
	 * @param p_batchSize
	 *            size of the buffer to store the vertex ids to send.
	 */
	public VerticesForNextFrontierMessage(final short p_destination, final int p_batchSize) {
		super(p_destination, BFSMessages.TYPE, BFSMessages.SUBTYPE_VERTICES_FOR_NEXT_FRONTIER_MESSAGE);

		m_vertexIDs = new long[p_batchSize];
	}

	public int getBatchSize() {
		return m_vertexIDs.length;
	}

	public int getNumVerticesInBatch() {
		return m_numOfVertices;
	}

	public void setNumVerticesInBatch(final int p_numVertsInBatch) {
		m_numOfVertices = p_numVertsInBatch;
	}

	public long[] getVertexIDBuffer() {
		return m_vertexIDs;
	}

	// Methods
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
		int size = ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode());

		return size + Integer.BYTES + m_numOfVertices * Long.BYTES;
	}
}
