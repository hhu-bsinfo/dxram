package de.uniduesseldorf.dxram.core.lookup.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.menet.AbstractResponse;

/**
 * Response to a PromotePeerRequest
 * @author Kevin Beineke
 *         06.09.2012
 */
public class PromotePeerResponse extends AbstractResponse {

	// Attributes
	private boolean m_success;

	// Constructors
	/**
	 * Creates an instance of PromotePeerResponse
	 */
	public PromotePeerResponse() {
		super();

		m_success = false;
	}

	/**
	 * Creates an instance of PromotePeerResponse
	 * @param p_request
	 *            the corresponding PromotePeerRequest
	 * @param p_success
	 *            whether promoting the peer was successful or not
	 */
	public PromotePeerResponse(final PromotePeerRequest p_request, final boolean p_success) {
		super(p_request, LookupMessages.SUBTYPE_PROMOTE_PEER_RESPONSE);

		m_success = p_success;
	}

	// Getters
	/**
	 * Get status
	 * @return whether promoting the peer was successful or not
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
