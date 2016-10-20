
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Lookup Request
 * @author Kevin Beineke
 *         06.09.2012
 */
public class GetLookupRangeRequest extends AbstractRequest {

	// Attributes
	private long m_chunkID;

	// Constructors
	/**
	 * Creates an instance of LookupRequest
	 */
	public GetLookupRangeRequest() {
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
	public GetLookupRangeRequest(final short p_destination, final long p_chunkID) {
		super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_GET_LOOKUP_RANGE_REQUEST);

		assert p_chunkID != ChunkID.INVALID_ID;

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
	protected final int getPayloadLength() {
		return Long.BYTES;
	}

}
