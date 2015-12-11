package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.menet.AbstractResponse;

/**
 * Response to a RemoveRequest
 * @author Florian Klein 09.03.2012
 */
public class RemoveResponse extends AbstractResponse {

	// Attributes
	private boolean m_success;

	// Constructors
	/**
	 * Creates an instance of RemoveResponse
	 */
	public RemoveResponse() {
		super();

		m_success = false;
	}

	/**
	 * Creates an instance of RemoveResponse
	 * @param p_request
	 *            the request
	 * @param p_success
	 *            true if remove was successful
	 */
	public RemoveResponse(final RemoveRequest p_request, final boolean p_success) {
		super(p_request, ChunkMessages.SUBTYPE_REMOVE_RESPONSE);

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
