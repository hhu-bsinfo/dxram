
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to the sign on request with status code if successful.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.05.16
 */
public class BarrierSignOnResponse extends AbstractResponse {
	private int m_barrierIdentifier = -1;

	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when receiving this message.
	 */
	public BarrierSignOnResponse() {
		super();
	}

	/**
	 * Creates an instance of SlaveSyncBarrierSignOnMessage.
	 * This constructor is used when sending this message.
	 * @param p_request
	 *            the request to respond to.
	 * @param p_status
	 *            Status code of the sign on
	 */
	public BarrierSignOnResponse(final BarrierSignOnRequest p_request, final byte p_status) {
		super(p_request, LookupMessages.SUBTYPE_BARRIER_SIGN_ON_RESPONSE);

		m_barrierIdentifier = p_request.getBarrierId();
		setStatusCode(p_status);
	}

	/**
	 * Id of the barrier
	 * @return Sync token.
	 */
	public int getBarrierId() {
		return m_barrierIdentifier;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_barrierIdentifier);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_barrierIdentifier = p_buffer.getInt();
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES;
	}
}
