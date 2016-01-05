package de.uniduesseldorf.dxram.core.lock.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.util.ChunkMessagesMetadataUtils;

import de.uniduesseldorf.menet.AbstractResponse;

/**
 * Response to a LockRequest
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 5.1.16
 */
public class LockResponse extends AbstractResponse {

	// Attributes
	private byte[] m_chunkStatusCodes = null;

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
	 * @param p_statusCodes Null if all locks were successful, otherwise an array with status codes for each chunkID.
	 */
	public LockResponse(final LockRequest p_request, final byte... p_statusCodes) {
		super(p_request, LockMessages.SUBTYPE_LOCK_RESPONSE);

		m_chunkStatusCodes = p_statusCodes;
		
		setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(getStatusCode(), p_statusCodes.length));
	}

	// Getters
	/**
	 * Get the status codes of the execution after message was received.
	 * @return Null if execution on all locks was successful, otherwise an array with error codes for each chunk requested.
	 */
	public final byte[] getStatusCodes() {
		return m_chunkStatusCodes;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_chunkStatusCodes.length);
		
		// null indicates that all calls were successful
		// send a single status byte only
		if (m_chunkStatusCodes == null) {
			p_buffer.put((byte) 1);
		} else {
			// otherwise send all the status bytes for further error evaluation later
			p_buffer.put((byte) 0);
			p_buffer.put(m_chunkStatusCodes);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int numChunks = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);
		boolean allSuccessful = p_buffer.get() == 1 ? true : false;
		
		// first byte indicates if all calls were successful, shortens message length
		// only get further bytes if failed
		if (!allSuccessful) {
			m_chunkStatusCodes = new byte[numChunks];
			
			p_buffer.get(m_chunkStatusCodes);
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode()) + m_chunkStatusCodes.length * Byte.BYTES;
	}

}
