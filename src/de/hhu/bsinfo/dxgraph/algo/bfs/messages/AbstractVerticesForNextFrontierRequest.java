
package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Base class for messages to send non local vertex data to another node for the next BFS depth level iteration.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 13.05.16
 */
public abstract class AbstractVerticesForNextFrontierRequest extends AbstractRequest {

	protected int m_numOfVertices;
	protected int m_batchSize;

	/**
	 * Creates an instance of VerticesForNextFrontierRequest.
	 * This constructor is used when receiving this message.
	 */
	public AbstractVerticesForNextFrontierRequest() {
		super();
	}

	/**
	 * Creates an instance of VerticesForNextFrontierRequest
	 * @param p_destination
	 *            the destination
	 * @param p_subtype
	 *            Subtype of the implemented message
	 * @param p_batchSize
	 *            size of the buffer to store the vertex ids to send.
	 */
	public AbstractVerticesForNextFrontierRequest(final short p_destination, final byte p_subtype,
			final int p_batchSize) {
		super(p_destination, BFSMessages.TYPE, p_subtype);

		m_batchSize = p_batchSize;
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
	 * Check if this batch is full.
	 * @return True if full, false if batch size not reached, yet.
	 */
	public boolean isBatchFull() {
		return m_batchSize == m_numOfVertices;
	}

	/**
	 * Add a vertex to this batch.
	 * @param p_vertex
	 *            Vertex Id to add.
	 * @return True if adding successful, false otherwise (batch full).
	 */
	public abstract boolean addVertex(final long p_vertex);

	/**
	 * Get the next vertex of this batch. An internal iterator/counter keeps
	 * track of the last gotten vertex of the batch. There is no possibility to
	 * reset the iterator, thus this is usable once.
	 * @return Vertex id of the next vertex or -1 if batch empty.
	 */
	public abstract long getVertex();
}
