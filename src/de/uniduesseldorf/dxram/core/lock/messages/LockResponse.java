package de.uniduesseldorf.dxram.core.lock.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.mem.ByteBufferDataStructureReaderWriter;
import de.uniduesseldorf.dxram.core.mem.Chunk;

import de.uniduesseldorf.menet.AbstractResponse;

/**
 * Response to a LockRequest
 * @author Florian Klein 09.03.2012
 */
public class LockResponse extends AbstractResponse {

	// Attributes
	private Chunk m_chunk = null;

	// Constructors
	/**
	 * Creates an instance of LockResponse
	 */
	public LockResponse() {
		super();
	}

	/**
	 * Creates an instance of LockResponse
	 * @param p_request
	 *            the corresponding LockRequest
	 * @param p_chunk
	 *            the requested Chunk
	 */
	public LockResponse(final LockRequest p_request, final Chunk p_chunk) {
		super(p_request, LockMessages.SUBTYPE_LOCK_RESPONSE);

		m_chunk = p_chunk;
	}

	// Getters
	/**
	 * Get the requested Chunk
	 * @return the requested Chunk
	 */
	public final Chunk getChunk() {
		return m_chunk;
	}

	// Methods
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
		LockRequest request = (LockRequest) getCorrespondingRequest();
		request.getDataStructure().writePayload(0, dataStructureWriter);
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Long.BYTES + m_chunk.sizeofPayload();
	}

}
