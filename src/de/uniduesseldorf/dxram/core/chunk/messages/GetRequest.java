package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.mem.DataStructure;
import de.uniduesseldorf.dxram.core.util.ChunkID;

import de.uniduesseldorf.menet.AbstractRequest;

/**
 * Request for getting a Chunk from a remote node
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class GetRequest extends AbstractRequest {

	// the data structure is stored for the sender of the request
	// to write the incoming data of the response to it
	private DataStructure m_dataStructure = null;
	// id is used when the message is sent and received
	private long m_id = ChunkID.INVALID_ID;

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
	public GetRequest(final short p_destination, final DataStructure p_dataStructure) {
		super(p_destination, ChunkMessages.TYPE, ChunkMessages.SUBTYPE_GET_REQUEST);

		m_dataStructure = p_dataStructure;
		m_id = p_dataStructure.getID();
	}
	
	/**
	 * Get the chunk ID of this request.
	 * @return Chunk ID.
	 */
	public long getChunkID() {
		return m_id;
	}
	
	/**
	 * Get the data structure stored with this request.
	 * This is used to write the received data to the provided object to avoid
	 * using multiple buffers.
	 * @return Data structure to store data to when the response arrived.
	 */
	DataStructure getDataStructure() {
		return m_dataStructure;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putLong(m_id);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_id = p_buffer.getLong();
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Long.BYTES;
	}

}
