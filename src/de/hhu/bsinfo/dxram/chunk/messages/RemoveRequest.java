package de.hhu.bsinfo.dxram.chunk.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Request for removing a Chunk on a remote node
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class RemoveRequest extends AbstractRequest {

	private Long[] m_chunkIDs = null;
	
	/**
	 * Creates an instance of RemoveRequest.
	 * This constructor is used when receiving this message.
	 */
	public RemoveRequest() {
		super();
	}

	/**
	 * Creates an instance of RemoveRequest.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination
	 * @param p_chunkID
	 *            the ID for the Chunk to remove
	 */
	public RemoveRequest(final short p_destination, final Long... p_chunkIds) {
		super(p_destination, ChunkMessages.TYPE, ChunkMessages.SUBTYPE_REMOVE_REQUEST);

		m_chunkIDs = p_chunkIds;
		
		setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(getStatusCode(), m_chunkIDs.length));
	}

	/**
	 * Get the ID for the Chunk to remove
	 * @return the ID for the Chunk to remove
	 */
	public final Long[] getChunkIDs() {
		return m_chunkIDs;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_chunkIDs.length);
		
		for (long chunkId : m_chunkIDs) {
			p_buffer.putLong(chunkId);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int numChunks = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);
		
		m_chunkIDs = new Long[numChunks];
		
		for (int i = 0; i < numChunks; i++) {
			m_chunkIDs[i] = p_buffer.getLong();
		}
	}

	@Override
	protected final int getPayloadLength() {
		return ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode()) + Long.BYTES * m_chunkIDs.length;
	}

}
