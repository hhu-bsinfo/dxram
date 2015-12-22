package de.uniduesseldorf.dxram.core.lock.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.util.ChunkID;

import de.uniduesseldorf.menet.AbstractMessage;

/**
 * Request for unlocking a Chunk on a remote node
 * @author Florian Klein 09.03.2012
 */
public class UnlockMessage extends AbstractMessage {

	// Attributes
	private long m_chunkID = ChunkID.INVALID_ID;

	// Constructors
	/**
	 * Creates an instance of UnlockRequest
	 */
	public UnlockMessage() {
		super();
	}

	/**
	 * Creates an instance of UnlockRequest
	 * @param p_destination
	 *            the destination
	 * @param p_chunkID
	 *            The ID of the Chunk to unlock
	 */
	public UnlockMessage(final short p_source, final short p_destination, final long p_chunkID) {
		super(p_source, p_destination, LockMessages.TYPE, LockMessages.SUBTYPE_UNLOCK_MESSAGE);

		ChunkID.check(p_chunkID);

		m_chunkID = p_chunkID;
	}

	// Getters
	/**
	 * Get the ID of the Chunk to lock
	 * @return the ID of the Chunk to lock
	 */
	public final long getChunkID() {
		return m_chunkID;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putLong(m_chunkID);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_chunkID = p_buffer.getLong();
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Long.BYTES;
	}

}
