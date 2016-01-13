package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to a InitRangeRequest
 * @author Kevin Beineke
 *         06.09.2012
 */
public class InitRangeResponse extends AbstractResponse {

	// Attributes
	private boolean m_success;

	// Constructors
	/**
	 * Creates an instance of InitRangeResponse
	 */
	public InitRangeResponse() {
		super();

		m_success = false;
	}

	/**
	 * Creates an instance of InitRangeResponse
	 * @param p_request
	 *            the corresponding InitRangeRequest
	 * @param p_success
	 *            whether the migration was successful or not
	 */
	public InitRangeResponse(final InitRangeRequest p_request, final boolean p_success) {
		super(p_request, LookupMessages.SUBTYPE_INIT_RANGE_RESPONSE);

		m_success = p_success;
	}

	// Getters
	/**
	 * Get the status
	 * @return whether the migration was successful or not
	 */
	public final boolean getStatus() {
		return m_success;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.put((byte) (m_success ? 1 : 0));
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_success = p_buffer.get() != 0 ? true : false;
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Byte.BYTES;
	}

}
