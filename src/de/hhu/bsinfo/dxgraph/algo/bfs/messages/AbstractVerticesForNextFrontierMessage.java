
package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

public abstract class AbstractVerticesForNextFrontierMessage extends AbstractMessage {

	protected int m_numOfVertices;
	protected int m_batchSize;

	/**
	 * Creates an instance of VerticesForNextFrontierMessage.
	 * This constructor is used when receiving this message.
	 */
	public AbstractVerticesForNextFrontierMessage() {
		super();
	}

	/**
	 * Creates an instance of VerticesForNextFrontierMessage
	 *
	 * @param p_destination the destination
	 * @param p_subtype     Subtype of the implemented message
	 * @param p_batchSize   size of the buffer to store the vertex ids to send.
	 */
	public AbstractVerticesForNextFrontierMessage(final short p_destination, final byte p_subtype,
			final int p_batchSize) {
		super(p_destination, BFSMessages.TYPE, p_subtype);

		m_batchSize = p_batchSize;
	}

	public int getBatchSize() {
		return m_batchSize;
	}

	public int getNumVerticesInBatch() {
		return m_numOfVertices;
	}

	public boolean isBatchFull() {
		return m_batchSize == m_numOfVertices;
	}

	public abstract boolean addVertex(final long p_vertex);

	public abstract long getVertex();

	public abstract void clear();
}
