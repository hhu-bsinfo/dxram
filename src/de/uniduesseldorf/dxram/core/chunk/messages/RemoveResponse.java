package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.menet.AbstractResponse;

/**
 * Response to a RemoveRequest
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class RemoveResponse extends AbstractResponse {

	private boolean m_success;

	/**
	 * Creates an instance of RemoveResponse.
	 * This constructor is used when receiving this message.
	 */
	public RemoveResponse() {
		super();

		m_success = false;
	}

	/**
	 * Creates an instance of RemoveResponse.
	 * This constructor is used when sending this message.
	 * @param p_request
	 *            the request
	 * @param p_success
	 *            true if remove was successful
	 */
	public RemoveResponse(final RemoveRequest p_request, final boolean p_success) {
		super(p_request, ChunkMessages.SUBTYPE_REMOVE_RESPONSE);

		m_success = p_success;
	}

	/**
	 * Get the status
	 * @return true if remove was successful
	 */
	public final boolean getStatus() {
		return m_success;
	}

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
