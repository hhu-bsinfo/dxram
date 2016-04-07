
package de.hhu.bsinfo.dxram.chunk.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.menet.AbstractResponse;

/**
 * Response to a RemoveRequest
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class RemoveResponse extends AbstractResponse {

	private byte[] m_chunkStatusCodes;

	/**
	 * Creates an instance of RemoveResponse.
	 * This constructor is used when receiving this message.
	 */
	public RemoveResponse() {
		super();
	}

	/**
	 * Creates an instance of RemoveResponse.
	 * This constructor is used when sending this message.
	 * @param p_request
	 *            the request
	 * @param p_statusCodes
	 *            status codes for every single chunk
	 */
	public RemoveResponse(final RemoveRequest p_request, final byte... p_statusCodes) {
		super(p_request, ChunkMessages.SUBTYPE_REMOVE_RESPONSE);

		m_chunkStatusCodes = p_statusCodes;

		setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(getStatusCode(), p_statusCodes.length));
	}

	/**
	 * Get the status codes of the removed chunks.
	 * @return Status codes of chunks.
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
	protected final int getPayloadLengthForWrite() {
		return ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode())
				+ m_chunkStatusCodes.length * Byte.BYTES;
	}

}
