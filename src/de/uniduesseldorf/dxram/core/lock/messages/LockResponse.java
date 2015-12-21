package de.uniduesseldorf.dxram.core.lock.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.data.Chunk;
import de.uniduesseldorf.dxram.core.data.DataStructure;
import de.uniduesseldorf.dxram.core.data.MessagesDataStructureImExporter;

import de.uniduesseldorf.menet.AbstractResponse;

/**
 * Response to a LockRequest
 * @author Florian Klein 09.03.2012
 */
public class LockResponse extends AbstractResponse {

	// Attributes
	private DataStructure m_dataStructure = null;

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
	public LockResponse(final LockRequest p_request, final DataStructure p_dataStructure) {
		super(p_request, LockMessages.SUBTYPE_LOCK_RESPONSE);

		m_dataStructure = p_dataStructure;
	}

	// Getters
	/**
	 * Get the requested DataStructure
	 * @return the requested DataStructure
	 */
	public final DataStructure getDataStructure() {
		return m_dataStructure;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		// read the data to be sent to the remote from the chunk set for this message
		MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
		int size = m_dataStructure.sizeofObject();
		
		// we don't need to re-send the id, the request still remembers that
		exporter.setPayloadSize(size);
		p_buffer.putInt(size);
		exporter.exportObject(m_dataStructure);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		// read the payload from the buffer and write it directly into
		// the data structure provided by the request to avoid further copying of data
		MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);
		LockRequest request = (LockRequest) getCorrespondingRequest();
		importer.setPayloadSize(p_buffer.getInt());
		importer.importObject(request.getDataStructure());
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Integer.BYTES + m_dataStructure.sizeofObject();
	}

}
