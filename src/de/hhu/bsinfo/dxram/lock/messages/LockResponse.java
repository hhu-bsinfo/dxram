package de.hhu.bsinfo.dxram.lock.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to a LockRequest
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 5.1.16
 */
public class LockResponse extends AbstractResponse {

	// Constructors
	/**
	 * Creates an instance of LockResponse as a receiver.
	 */
	public LockResponse() {
		super();
	}

	/**
	 * Creates an instance of LockResponse as a sender.
	 * @param p_request Corresponding request to this response.
	 * @param p_statusCode Status code for locking the chunk.
	 */
	public LockResponse(final LockRequest p_request, final byte p_statusCode) {
		super(p_request, LockMessages.SUBTYPE_LOCK_RESPONSE);

		setStatusCode(p_statusCode);
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return 0;
	}

}
