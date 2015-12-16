package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.mem.DataStructure;

import de.uniduesseldorf.menet.AbstractRequest;

/**
 * Request for getting a Chunk from a remote node
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class GetRequest extends AbstractRequest {

	// the data structure is stored for the sender of the request
	// to write the incoming data of the response to it
	// the requesting IDs are taken from the structures
	private DataStructure[] m_dataStructure = null;
	// this is only used when receiving the request
	private long[] m_chunkIDs = null;

	/**
	 * Creates an instance of GetRequest.
	 * This constructor is used when receiving this message.
	 */
	public GetRequest() {
		super();
	}

	/**
	 * Creates an instance of GetRequest.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 * @param p_dataStructure
	 *            Data structure with the ID of the chunk to get.
	 */
	public GetRequest(final short p_destination, final boolean p_lockAcquire, final DataStructure... p_dataStructures) {
		super(p_destination, ChunkMessages.TYPE, ChunkMessages.SUBTYPE_GET_REQUEST);

		m_dataStructure = p_dataStructures;
		
		byte tmpCode = ChunkMessagesUtils.setLockFlag(getStatusCode(), p_lockAcquire);
		ChunkMessagesUtils.setNumberOfItemsToSend(tmpCode, p_dataStructures.length);
		setStatusCode(tmpCode);
	}
	
	/**
	 * Get the chunk IDs of this request (when receiving it).
	 * @return Chunk ID.
	 */
	public long[] getChunkIDs() {
		return m_chunkIDs;
	}
	
	/**
	 * Get the acquire lock flag of the request (when receiving it).
	 * @return
	 */
	public boolean acquireLock() {
		return ChunkMessagesUtils.getLockFlag(getStatusCode());
	}
	
	/**
	 * Get the data structures stored with this request.
	 * This is used to write the received data to the provided object to avoid
	 * using multiple buffers.
	 * @return Data structures to store data to when the response arrived.
	 */
	DataStructure[] getDataStructures() {
		return m_dataStructure;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		ChunkMessagesUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_dataStructure.length);
		
		for (DataStructure dataStructure : m_dataStructure) {
			p_buffer.putLong(dataStructure.getID());
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int numChunks = ChunkMessagesUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);
		
		m_chunkIDs = new long[numChunks];
		for (int i = 0; i < m_chunkIDs.length; i++) {
			m_chunkIDs[i] = p_buffer.getLong();
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return ChunkMessagesUtils.getSizeOfAdditionalLengthField(getStatusCode()) + Long.BYTES * m_dataStructure.length;
	}
}
