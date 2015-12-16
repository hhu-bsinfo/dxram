package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.menet.AbstractResponse;

/**
 * Response to a PutRequest
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class PutResponse extends AbstractResponse {

	private byte[] m_chunkStatusCodes = null;

	/**
	 * Creates an instance of PutResponse.
	 * This constructor is used when receiving this message.
	 */
	public PutResponse() {
		super();
	}

	/**
	 * Creates an instance of DataResponse.
	 * This constructor is used when sending this message.
	 * @param p_request
	 *            the request
	 * @param p_success
	 *            true if put was successful
	 */
	public PutResponse(final PutRequest p_request, final byte... p_statusCodes) {
		super(p_request, ChunkMessages.SUBTYPE_PUT_RESPONSE);

		m_chunkStatusCodes = p_statusCodes;
		
		setStatusCode(ChunkMessagesUtils.setNumberOfItemsToSend(getStatusCode(), p_statusCodes.length));
	}

	/**
	 * Get the status
	 * @return true if put was successful
	 */
	public final byte[] getStatusCodes() {
		return m_chunkStatusCodes;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		ChunkMessagesUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_chunkStatusCodes.length);
		
		p_buffer.put(m_chunkStatusCodes);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int numChunks = ChunkMessagesUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);
		
		m_chunkStatusCodes = new byte[numChunks];
		
		p_buffer.get(m_chunkStatusCodes);
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return ChunkMessagesUtils.getSizeOfAdditionalLengthField(getStatusCode()) + m_chunkStatusCodes.length * Byte.BYTES;
	}

}