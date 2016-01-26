package de.hhu.bsinfo.dxram.chunk.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.menet.AbstractResponse;

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
		setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(getStatusCode(), p_numChunksGot));
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
		ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_dataStructures.length);
		
		// read the data to be sent to the remote from the chunk set for this message
		MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
		for (DataStructure dataStructure : m_dataStructures) {
			int size = dataStructure.sizeofObject();
			// we keep the order of the chunks, so we don't have to send the ID again
			//p_buffer.putLong(dataStructure.getID());
			exporter.setPayloadSize(size);
			p_buffer.putInt(size);
			exporter.exportObject(dataStructure);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_numChunksGot = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);
		
		// read the payload from the buffer and write it directly into
		// the data structure provided by the request to avoid further copying of data
		MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);
		GetRequest request = (GetRequest) getCorrespondingRequest();
		for (DataStructure dataStructure : request.getDataStructures()) {
			importer.setPayloadSize(p_buffer.getInt());
			importer.importObject(dataStructure);
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		int size = ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode());
		
		size += m_dataStructures.length * Integer.BYTES;
		
		for (DataStructure dataStructure : m_dataStructures) {
			size += dataStructure.sizeofObject();
		}
		
		return size;
	}

}
