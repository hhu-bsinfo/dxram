package de.uniduesseldorf.dxram.core.lookup.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.menet.AbstractRequest;
import de.uniduesseldorf.utils.Contract;

/**
 * Lookup Request
 * @author Kevin Beineke
 *         06.09.2012
 */
public class LookupRequest extends AbstractRequest {

	// Attributes
	private long m_chunkID;

	// Constructors
	/**
	 * Creates an instance of LookupRequest
	 */
	public LookupRequest() {
		super();

		m_chunkID = -1;
	}

	/**
	 * Creates an instance of LookupRequest
	 * @param p_destination
	 *            the destination
	 * @param p_chunkID
	 *            the ChunkID of the requested object
	 */
	public LookupRequest(final short p_destination, final long p_chunkID) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_LOOKUP_REQUEST);

		Contract.checkNotNull(p_chunkID, "no ChunkID given");

		m_chunkID = p_chunkID;
	}

	// Getters
	/**
	 * Get the ChunkID
	 * @return the ChunkID
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
