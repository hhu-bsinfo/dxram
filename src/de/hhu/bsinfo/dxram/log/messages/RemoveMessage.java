package de.hhu.bsinfo.dxram.log.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Message for removing a Chunk on a remote node
 * @author Kevin Beineke 20.04.2014
 */
public class RemoveMessage extends AbstractMessage {

	// Attributes
	private Long[] m_chunkIDs;
	private byte m_rangeID;
	private ByteBuffer m_buffer;

	// Constructors
	/**
	 * Creates an instance of RemoveMessage
	 */
	public RemoveMessage() {
		super();

		m_chunkIDs = null;
		m_rangeID = -1;
		m_buffer = null;
	}

	/**
	 * Creates an instance of RemoveMessage
	 * @param p_destination
	 *            the destination
	 * @param ids
	 *            the ChunkIDs of the Chunks to remove
	 */
	public RemoveMessage(final short p_destination, final Long[] ids) {
		super(p_destination, LogMessages.TYPE, LogMessages.SUBTYPE_REMOVE_MESSAGE, true);

		m_chunkIDs = ids;
		m_rangeID = -1;
	}
	
	/**
	 * Creates an instance of RemoveMessage
	 * @param p_destination
	 *            the destination
	 * @param ids
	 *            the ChunkIDs of the Chunks to remove
	 */
	public RemoveMessage(final short p_destination, final long[] ids) {
		super(p_destination, LogMessages.TYPE, LogMessages.SUBTYPE_REMOVE_MESSAGE, true);

		Long[] chunkIDs = new Long[ids.length];
		for (int i = 0; i < ids.length; i++) {
			chunkIDs[i] = ids[i];
		}
		m_chunkIDs = chunkIDs;
		m_rangeID = -1;
	}

	/**
	 * Creates an instance of RemoveMessage
	 * @param p_destination
	 *            the destination
	 * @param p_chunkIDs
	 *            the ChunkIDs of the Chunks to remove
	 * @param p_rangeID
	 *            the RangeID
	 */
	public RemoveMessage(final short p_destination, final Long[] p_chunkIDs, final byte p_rangeID) {
		super(p_destination, LogMessages.TYPE, LogMessages.SUBTYPE_REMOVE_MESSAGE, true);

		m_chunkIDs = p_chunkIDs;
		m_rangeID = p_rangeID;
	}

	// Getters
	/**
	 * Get the RangeID
	 * @return the RangeID
	 */
	public final ByteBuffer getMessageBuffer() {
		return m_buffer;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.put(m_rangeID);
		p_buffer.putInt(m_chunkIDs.length);
		for (int i = 0; i < m_chunkIDs.length; i++) {
			p_buffer.putLong(m_chunkIDs[i]);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_buffer = p_buffer;
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Byte.BYTES + Integer.BYTES + (Long.BYTES * m_chunkIDs.length);
	}
}
