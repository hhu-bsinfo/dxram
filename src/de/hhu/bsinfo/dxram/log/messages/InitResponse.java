
package de.hhu.bsinfo.dxram.log.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to a InitRequest
 * @author Kevin Beineke 20.04.2014
 */
public class InitResponse extends AbstractResponse {

	// Attributes
	private boolean m_success;

	// Constructors
	/**
	 * Creates an instance of InitResponse
	 */
	public InitResponse() {
		super();

		m_success = false;
	}

	/**
	 * Creates an instance of InitResponse
	 * @param p_request
	 *            the request
	 * @param p_success
	 *            true if remove was successful
	 */
	public InitResponse(final InitRequest p_request, final boolean p_success) {
		super(p_request, LogMessages.SUBTYPE_INIT_RESPONSE);

		m_success = p_success;
	}

	// Getters
	/**
	 * Get the status
	 * @return true if remove was successful
	 */
	public final boolean getStatus() {
		return m_success;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		if (m_success) {
			p_buffer.put((byte) 1);
		} else {
			p_buffer.put((byte) 0);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		final byte b = p_buffer.get();
		if (b == 1) {
			m_success = true;
		} else {
			m_success = false;
		}
	}

	@Override
	protected final int getPayloadLength() {
		return Byte.BYTES;
	}

}
