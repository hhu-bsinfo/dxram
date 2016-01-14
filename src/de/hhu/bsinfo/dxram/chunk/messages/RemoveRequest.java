package de.hhu.bsinfo.dxram.chunk.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.util.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Request for removing a Chunk on a remote node
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class RemoveRequest extends AbstractRequest {

	// used when sending the remove request, only chunk id
	// is transfered
	private DataStructure[] m_dataStructures = null;
	// used when receiving the message
	private long[] m_chunkIDs = null;
	
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
	public RemoveRequest(final short p_destination, final DataStructure... p_dataStructures) {
		super(p_destination, ChunkMessages.TYPE, ChunkMessages.SUBTYPE_REMOVE_REQUEST);

		m_dataStructures = p_dataStructures;
		
		setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(getStatusCode(), p_dataStructures.length));
	}

	/**
	 * Get the ID for the Chunk to remove
	 * @return the ID for the Chunk to remove
	 */
	public final long[] getChunkIDs() {
		return m_chunkIDs;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_dataStructures.length);
		
		for (DataStructure dataStructure : m_dataStructures) {
			p_buffer.putLong(dataStructure.getID());
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int numChunks = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);
		
		m_chunkIDs = new long[numChunks];
		
		for (int i = 0; i < numChunks; i++) {
			m_chunkIDs[i] = p_buffer.getLong();
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode()) + Long.BYTES * m_dataStructures.length;
	}

}
