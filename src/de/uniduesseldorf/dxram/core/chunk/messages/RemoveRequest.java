package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.util.ChunkID;

import de.uniduesseldorf.menet.AbstractRequest;

/**
 * Request for removing a Chunk on a remote node
 * @author Florian Klein 09.03.2012
 */
public class RemoveRequest extends AbstractRequest {

	// Attributes
	private long m_chunkID;

	// Constructors
	/**
	 * Creates an instance of RemoveRequest
	 */
	public RemoveRequest() {
		super();

		m_chunkID = ChunkID.INVALID_ID;
	}

	/**
	 * Creates an instance of RemoveRequest
	 * @param p_destination
	 *            the destination
	 * @param p_chunkID
	 *            the ID for the Chunk to remove
	 */
	public RemoveRequest(final short p_destination, final long p_chunkID) {
		super(p_destination, ChunkMessages.TYPE, ChunkMessages.SUBTYPE_REMOVE_REQUEST);

		ChunkID.check(p_chunkID);

		m_chunkID = p_chunkID;
	}

	// Getters
	/**
	 * Get the ID for the Chunk to remove
	 * @return the ID for the Chunk to remove
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
