package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.mem.ByteBufferDataStructureReaderWriter;
import de.uniduesseldorf.dxram.core.mem.DataStructure;

import de.uniduesseldorf.menet.AbstractResponse;

/**
 * Response to a MultiGetRequest
 * @author Florian Klein 05.07.2014
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class MultiGetResponse extends AbstractResponse {

	// only used for sending: buffered structures to send for get response
	// the resulting data is stored in the DataStructures of the request
	private DataStructure[] m_dataStructures = null;
	
	// Constructors
	/**
	 * Creates an instance of MultiGetResponse.
	 * This constructor is used when receiving this message.
	 */
	public MultiGetResponse() {
		super();
	}

	/**
	 * Creates an instance of MultiGetResponse.
	 * This constructor is used when sending this message.
	 * @param p_request
	 *            the corresponding MultiGetRequest
	 * @param p_dataStructures
	 *            The data structures containing the data to send.
	 */
	public MultiGetResponse(final MultiGetRequest p_request, final DataStructure[] p_dataStructures) {
		super(p_request, ChunkMessages.SUBTYPE_MULTIGET_RESPONSE);

		m_dataStructures = p_dataStructures;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		ByteBufferDataStructureReaderWriter dataStructureWriter = new ByteBufferDataStructureReaderWriter(p_buffer);
		
		p_buffer.putInt(m_dataStructures.length);
		for (DataStructure dataStructure : m_dataStructures)
		{
			p_buffer.putLong(dataStructure.getID());
			dataStructure.writePayload(0, dataStructureWriter);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		// read the payload from the buffer and write it directly into
		// the data structure provided by the request to avoid further copying of data
		int numChunks = p_buffer.getInt();
		MultiGetRequest request = (MultiGetRequest) getCorrespondingRequest();
		DataStructure[] dataStructures = request.getDataStructures();
		ByteBufferDataStructureReaderWriter dataStructureWriter = new ByteBufferDataStructureReaderWriter(p_buffer);
		
		for (int i = 0; i < numChunks; i++) {
			// omit ID, because the order is kept
			p_buffer.getLong();
			dataStructures[i].writePayload(0, dataStructureWriter);
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		int length = Integer.BYTES;
		
		for (DataStructure dataStructure : m_dataStructures) {
			length += Long.BYTES + dataStructure.sizeofPayload();
		}
		
		return length;
	}

}