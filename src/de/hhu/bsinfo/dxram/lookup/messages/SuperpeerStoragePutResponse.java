package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.menet.AbstractResponse;

import java.nio.ByteBuffer;

/**
 * Response to the put request.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 17.05.15
 */
public class SuperpeerStoragePutResponse extends AbstractResponse {
	private byte[] m_chunkStatusCodes;

	/**
	 * Creates an instance of SuperpeerStoragePutResponse.
	 * This constructor is used when receiving this message.
	 */
	public SuperpeerStoragePutResponse() {
		super();
	}

	/**
	 * Creates an instance of SuperpeerStoragePutResponse.
	 * This constructor is used when sending this message.
	 *
	 * @param p_request     the request
	 * @param p_statusCodes Status code for every single chunk put.
	 */
	public SuperpeerStoragePutResponse(final SuperpeerStoragePutRequest p_request, final byte... p_statusCodes) {
		super(p_request, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_RESPONSE);

		m_chunkStatusCodes = p_statusCodes;

		setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(getStatusCode(), p_statusCodes.length));
	}

	/**
	 * Get the status
	 *
	 * @return true if put was successful
	 */
	public final byte[] getStatusCodes() {
		return m_chunkStatusCodes;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer,
				m_chunkStatusCodes.length);

		p_buffer.put(m_chunkStatusCodes);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int numChunks = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);

		m_chunkStatusCodes = new byte[numChunks];

		p_buffer.get(m_chunkStatusCodes);
	}

	@Override
	protected final int getPayloadLength() {
		return ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode())
				+ m_chunkStatusCodes.length * Byte.BYTES;
	}
}
