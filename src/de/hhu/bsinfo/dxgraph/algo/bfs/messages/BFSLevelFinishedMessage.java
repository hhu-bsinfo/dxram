package de.hhu.bsinfo.dxgraph.algo.bfs.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Message broadcasted by one bfs peer to all other participating peers when
 * the current peer has finished his iteration.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 19.05.16
 */
public class BFSLevelFinishedMessage extends AbstractMessage {

	private int m_token;
	private long m_sentMsgCount;
	private long m_receivedMsgCount;

	/**
	 * Creates an instance of BFSLevelFinishedMessage.
	 * This constructor is used when receiving this message.
	 */
	public BFSLevelFinishedMessage() {
		super();
	}

	/**
	 * Creates an instance of BFSLevelFinishedMessage
	 *
	 * @param p_destination the destination
	 */
	public BFSLevelFinishedMessage(final short p_destination, final int p_token, final long p_sentMsgCount,
			final long p_receivedMsgCount) {
		super(p_destination, BFSMessages.TYPE, BFSMessages.SUBTYPE_BFS_LEVEL_FINISHED_MESSAGE);

		m_token = p_token;
		m_sentMsgCount = p_sentMsgCount;
		m_receivedMsgCount = p_receivedMsgCount;
	}

	public int getToken() {
		return m_token;
	}

	public long getSentMessageCount() {
		return m_sentMsgCount;
	}

	public long getReceivedMessageCount() {
		return m_receivedMsgCount;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_token);
		p_buffer.putLong(m_sentMsgCount);
		p_buffer.putLong(m_receivedMsgCount);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_token = p_buffer.getInt();
		m_sentMsgCount = p_buffer.getLong();
		m_receivedMsgCount = p_buffer.getLong();
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES + 2 * Long.BYTES;
	}
}
