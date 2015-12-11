package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.mem.ByteBufferDataStructureReaderWriter;
import de.uniduesseldorf.dxram.core.mem.Chunk;

import de.uniduesseldorf.menet.AbstractResponse;

/**
 * Response to a GetRequest
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class GetResponse extends AbstractResponse {

	// The chunk object here is only used when sending the response
	// when the response is received, the data structure from the request is
	// used to directly write the data to it and avoiding further copying
	private Chunk m_chunk = null;

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
	 * @param p_request
	 *            the corresponding GetRequest
	 * @param p_chunk
	 *            the requested Chunk
	 */
	public GetResponse(final GetRequest p_request, final Chunk p_chunk) {
		super(p_request, ChunkMessages.SUBTYPE_GET_RESPONSE);

		m_chunk = p_chunk;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		// read the data to be sent to the remote from the chunk set for this message
		ByteBufferDataStructureReaderWriter dataStructureWriter = new ByteBufferDataStructureReaderWriter(p_buffer);
		p_buffer.putLong(m_chunk.getID());
		m_chunk.writePayload(0, dataStructureWriter);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		// read the payload from the buffer and write it directly into
		// the data structure provided by the request to avoid further copying of data
		ByteBufferDataStructureReaderWriter dataStructureWriter = new ByteBufferDataStructureReaderWriter(p_buffer);
		GetRequest request = (GetRequest) getCorrespondingRequest();
		request.getDataStructure().writePayload(0, dataStructureWriter);
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Long.BYTES + m_chunk.sizeofPayload();
	}

}
