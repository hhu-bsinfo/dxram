package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.mem.ByteBufferDataStructureReaderWriter;
import de.uniduesseldorf.dxram.core.mem.DataStructure;

import de.uniduesseldorf.menet.AbstractResponse;

/**
 * Response to a GetRequest
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class GetResponse extends AbstractResponse {

	// The chunk objects here are used when sending the response only
	// when the response is received, the data structures from the request are
	// used to directly write the data to them and avoiding further copying
	private DataStructure[] m_dataStructures = null;

	// when receiving the repsonse, tells us how many chunks were successfully
	// grabbed from the remote machine
	private int m_numChunksGot = 0;
	
	/**
	 * Creates an instance of GetResponse.
	 * This constructor is used when receiving this message.
	 */
	public GetResponse() {
		super();
	}

	/**
	 * Creates an instance of GetResponse.
	 * This constructor is used when sending this message.
	 * Make sure to include all the chunks with IDs from the request in the correct order. If a chunk does
	 * not exist, no data and a length of 0 indicates this situation. 
	 * @param p_request
	 *            the corresponding GetRequest
	 * @param p_chunk
	 *            the requested Chunk
	 */
	public GetResponse(final GetRequest p_request, final int p_numChunksGot, final DataStructure... p_dataStructures) {
		super(p_request, ChunkMessages.SUBTYPE_GET_RESPONSE);

		m_dataStructures = p_dataStructures;
		setStatusCode(ChunkMessagesUtils.setNumberOfItemsToSend(getStatusCode(), p_numChunksGot));
	}
	
	/**
	 * Tells how many chunks have successfully been retrieved from the remote machine.
	 * @return Number of chunks retrieved from remote machine.
	 */
	public int getNumberOfChunksGot() {
		return m_numChunksGot;
	}
	
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		ChunkMessagesUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_dataStructures.length);
		
		// read the data to be sent to the remote from the chunk set for this message
		ByteBufferDataStructureReaderWriter dataStructureWriter = new ByteBufferDataStructureReaderWriter(p_buffer);
		for (DataStructure dataStructure : m_dataStructures) {
			// we keep the order of the chunks, so we don't have to send the ID again
			//p_buffer.putLong(dataStructure.getID());
			p_buffer.putInt(dataStructure.sizeofPayload());
			dataStructure.writePayload(0, dataStructureWriter);	
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_numChunksGot = ChunkMessagesUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);
		
		// read the payload from the buffer and write it directly into
		// the data structure provided by the request to avoid further copying of data
		ByteBufferDataStructureReaderWriter dataStructureWriter = new ByteBufferDataStructureReaderWriter(p_buffer);
		GetRequest request = (GetRequest) getCorrespondingRequest();
		for (DataStructure dataStructure : request.getDataStructures()) {
			dataStructure.readPayload(0, p_buffer.getInt(), dataStructureWriter);
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		int size = ChunkMessagesUtils.getSizeOfAdditionalLengthField(getStatusCode());
		
		size += m_dataStructures.length * Integer.BYTES;
		
		for (DataStructure dataStructure : m_dataStructures) {
			size += dataStructure.sizeofPayload();
		}
		
		return size;
	}

}
