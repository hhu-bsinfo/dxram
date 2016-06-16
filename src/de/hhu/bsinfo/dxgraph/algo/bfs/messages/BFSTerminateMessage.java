package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Created by nothaas on 6/2/16.
 */
public class BFSTerminateMessage extends AbstractMessage {
	private long m_frontierNextVerices;
	private long m_frontierNextEdges;

	/**
	 * Creates an instance of BFSTerminateMessage.
	 * This constructor is used when receiving this message.
	 */
	public BFSTerminateMessage() {
		super();
	}

	/**
	 * Creates an instance of BFSTerminateMessage
	 *
	 * @param p_destination the destination
	 */
	public BFSTerminateMessage(final short p_destination, final long p_frontierNextVertices,
			final long p_frontierNextEdges) {
		super(p_destination, BFSMessages.TYPE, BFSMessages.SUBTYPE_BFS_TERMINATE_MESSAGE);

		m_frontierNextVerices = p_frontierNextVertices;
		m_frontierNextEdges = p_frontierNextEdges;
	}

	public long getFrontierNextVertices() {
		return m_frontierNextVerices;
	}

	public long getFrontierNextEdges() {
		return m_frontierNextEdges;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putLong(m_frontierNextVerices);
		p_buffer.putLong(m_frontierNextEdges);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_frontierNextVerices = p_buffer.getLong();
		m_frontierNextEdges = p_buffer.getLong();
	}

	@Override
	protected final int getPayloadLength() {
		return 2 * Long.BYTES;
	}
}
