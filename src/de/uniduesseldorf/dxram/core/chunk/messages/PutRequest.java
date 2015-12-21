package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.mem.ByteBufferDataStructureReaderWriter;
import de.uniduesseldorf.dxram.core.mem.Chunk;
import de.uniduesseldorf.dxram.core.mem.DataStructure;

import de.uniduesseldorf.menet.AbstractRequest;

/**
 * Request for updating a Chunk on a remote node
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class PutRequest extends AbstractRequest {

	// DataStructures used when sending the put request.
	// These are also used by the response to directly write the
	// receiving data to the structures
	// Chunks are created and used when receiving a put request
	private DataStructure[] m_dataStructures = null;

	/**
	 * Creates an instance of PutRequest.
	 * This constructor is used when receiving this message.
	 */
	public PutRequest() {
		super();
	}

	/**
	 * Creates an instance of PutRequest.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination
	 * @param p_dataStructure
	 *            Data structure with the data to put.
	 */
	public PutRequest(final short p_destination, final DataStructure... p_dataStructures) {
		this(p_destination, false, p_dataStructures);
	}

	/**
	 * Creates an instance of PutRequest
	 * @param p_destination
	 *            the destination
	 * @param p_dataStructure
	 *            Data structure with the data to put.
	 * @param p_releaseLock
	 *            if true a potential lock will be released
	 */
	public PutRequest(final short p_destination, final boolean p_releaseLock, final DataStructure... p_dataStructures) {
		super(p_destination, ChunkMessages.TYPE, ChunkMessages.SUBTYPE_PUT_REQUEST);

		m_dataStructures = p_dataStructures;
		
		byte tmpCode = ChunkMessagesUtils.setLockFlag(getStatusCode(), p_releaseLock);
		ChunkMessagesUtils.setNumberOfItemsToSend(tmpCode, p_dataStructures.length);
		setStatusCode(tmpCode);
	}

	/**
	 * Get the DataStructures to put when this message is received.
	 * @return the Chunk to put
	 */
	public final DataStructure[] getDataStructures() {
		return m_dataStructures;
	}

	/**
	 * Checks if a potential lock should be released
	 * @return true if a potential lock should be released, false otherwise
	 */
	public final boolean isReleaseLock() {
		return ChunkMessagesUtils.getLockFlag(getStatusCode());
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		ChunkMessagesUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_dataStructures.length);
		
		ByteBufferDataStructureReaderWriter dataStructureWriter = new ByteBufferDataStructureReaderWriter(p_buffer);
		for (DataStructure dataStructure : m_dataStructures) {
			p_buffer.putLong(dataStructure.getID());
			p_buffer.putInt(dataStructure.sizeofPayload());
			dataStructure.writePayload(0, dataStructureWriter);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		ByteBufferDataStructureReaderWriter dataStructureReader = new ByteBufferDataStructureReaderWriter(p_buffer);
		int numChunks = ChunkMessagesUtils.getSizeOfAdditionalLengthField(getStatusCode());
		
		m_dataStructures = new Chunk[numChunks];
		
		for (int i = 0; i < m_dataStructures.length; i++) {
			long id = p_buffer.getLong();
			int size = p_buffer.getInt();
			
			m_dataStructures[i] = new Chunk(id, size);
			m_dataStructures[i].readPayload(0, size, dataStructureReader);
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {	
		int size = ChunkMessagesUtils.getSizeOfAdditionalLengthField(getStatusCode());
		
		size += m_dataStructures.length * Long.BYTES;
		size += m_dataStructures.length * Integer.BYTES;
		
		for (DataStructure dataStructure : m_dataStructures) {
			size += dataStructure.sizeofPayload();
		}
		
		return size;
	}

}
