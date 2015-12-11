package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;
import java.util.Vector;

import de.uniduesseldorf.dxram.core.mem.ByteBufferDataStructureReaderWriter;
import de.uniduesseldorf.dxram.core.mem.DataStructure;

import de.uniduesseldorf.menet.AbstractResponse;

/**
 * Response to a MultiGetRequest
 * @author Florian Klein 05.07.2014
 */
public class MultiGetResponse extends AbstractResponse {

	// Attributes
	// only used for sending: buffered structures to send for get response
	private DataStructure[] m_dataStructures = null;
	// only used for receiving: list of IDs with failed get request
	private Vector<Long> m_failedGetIDs = null;
	
	// Constructors
	/**
	 * Creates an instance of MultiGetResponse
	 */
	public MultiGetResponse() {
		super();
	}

	/**
	 * Creates an instance of MultiGetResponse
	 * @param p_request
	 *            the corresponding MultiGetRequest
	 * @param p_chunks
	 *            the requested Chunks
	 */
	public MultiGetResponse(final MultiGetRequest p_request, final DataStructure[] p_dataStructures) {
		super(p_request, ChunkMessages.SUBTYPE_MULTIGET_RESPONSE);

		m_dataStructures = p_dataStructures;
	}
	
	/**
	 * When receiving the response, get a list of IDs that failed
	 * for the initial request.
	 * @return List of failed IDs or null if all get requests were successful.
	 */
	public Vector<Long> getFailedGetIDs()
	{
		return m_failedGetIDs;
	}

	// Methods
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
		
		// overall check if getting all chunks was successful
		if (numChunks == dataStructures.length) {			
			for (int i = 0; i < numChunks; i++) {
				// omit ID, because the order is kept
				p_buffer.getLong();
				dataStructures[i].writePayload(0, dataStructureWriter);
			}
		} else {
			int posDataStructures = 0;
			
			m_failedGetIDs = new Vector<Long>();
			
			// check against all IDs (slow) to figure out which IDs failed
			// -> the order of successful elements is kept, just the failed ones left gaps
			for (int i = 0; i < numChunks; i++) {
				long id = p_buffer.getLong();
				DataStructure dataStructure = null;
				
				while (dataStructure == null && posDataStructures != dataStructures.length) {
					if (dataStructures[posDataStructures].getID() == id) {
						dataStructure = dataStructures[posDataStructures];
					}
					
					posDataStructures++;
				}
				
				if (dataStructure == null) {
					m_failedGetIDs.add(id);
				} else {
					dataStructure.writePayload(0, dataStructureWriter);
				}
			}
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