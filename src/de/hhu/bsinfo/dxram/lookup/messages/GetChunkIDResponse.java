package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.util.ChunkID;
import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to a GetChunkIDRequest
 * @author Florian Klein
 *         09.03.2012
 */
public class GetChunkIDResponse extends AbstractResponse {

	// Attributes
	private long m_chunkID;

	// Constructors
	/**
	 * Creates an instance of GetChunkIDResponse
	 */
	public GetChunkIDResponse() {
		super();

		m_chunkID = ChunkID.INVALID_ID;
	}

	/**
	 * Creates an instance of GetChunkIDResponse
	 * @param p_request
	 *            the request
	 * @param p_chunkID
	 *            the ChunkID
	 */
	public GetChunkIDResponse(final GetChunkIDRequest p_request, final long p_chunkID) {
		super(p_request, LookupMessages.SUBTYPE_GET_CHUNKID_RESPONSE);

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
