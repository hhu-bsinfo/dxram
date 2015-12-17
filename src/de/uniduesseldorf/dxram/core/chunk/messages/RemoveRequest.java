package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.data.DataStructure;
import de.uniduesseldorf.menet.AbstractRequest;

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
		
		setStatusCode(ChunkMessagesUtils.setNumberOfItemsToSend(getStatusCode(), p_dataStructures.length));
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
		ChunkMessagesUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_dataStructures.length);
		
		for (DataStructure dataStructure : m_dataStructures) {
			p_buffer.putLong(dataStructure.getID());
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int numChunks = ChunkMessagesUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);
		
		m_chunkIDs = new long[numChunks];
		
		p_buffer.asLongBuffer().get(m_chunkIDs);
		// we have to manually advance the original buffer
		p_buffer.position(p_buffer.position() + numChunks * Long.BYTES);
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return ChunkMessagesUtils.getSizeOfAdditionalLengthField(getStatusCode()) + Long.BYTES * m_dataStructures.length;
	}

}
