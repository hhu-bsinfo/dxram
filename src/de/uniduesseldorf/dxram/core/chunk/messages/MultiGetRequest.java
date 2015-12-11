package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.mem.DataStructure;

import de.uniduesseldorf.menet.AbstractRequest;

/**
 * Request for getting multiple Chunks from a remote node
 * @author Florian Klein 05.07.2014
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class MultiGetRequest extends AbstractRequest {

	// data structures affected by the request are stored, so the
	// response can write directly to them
	private DataStructure[] m_dataStructures = null;
	// this is only used when receiving the request
	private long[] m_chunkIDs = null;

	/**
	 * Creates an instance of MultiGetRequest.
	 * This constructor is used when receiving this message.
	 */
	public MultiGetRequest() {
		super();
	}

	/**
	 * Creates an instance of MultiGetRequest.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination
	 * @param p_dataStructures
	 *            The data structures with IDs to get the data for.
	 */
	public MultiGetRequest(final short p_destination, final DataStructure[] p_dataStructures) {
		super(p_destination, ChunkMessages.TYPE, ChunkMessages.SUBTYPE_MULTIGET_REQUEST);
		m_dataStructures = p_dataStructures;
		
		m_chunkIDs = new long[m_dataStructures.length];
		for (int i = 0; i < m_chunkIDs.length; i++) {
			m_chunkIDs[i] = m_dataStructures[i].getID();
		}
	}
	
	/**
	 * Get the IDs of the Chunks to get
	 * @return the IDs of the Chunks to get
	 */
	public final long[] getChunkIDs() {
		return m_chunkIDs;
	}
	
	/**
	 * Get the data structures of this request. This is used by the initial sender
	 * when receiving the response to write the received data directly to the
	 * data structures instead of buffering it again.
	 * @return The data structures to write the resulting data to.
	 */
	DataStructure[] getDataStructures()
	{
		return m_dataStructures;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_chunkIDs.length);
		for (long id : m_chunkIDs) {
			p_buffer.putLong(id);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_chunkIDs = new long[p_buffer.getInt()];
		
		for (int i = 0; i < m_chunkIDs.length; i++) {
			m_chunkIDs[i] = p_buffer.getLong();
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Integer.BYTES + m_chunkIDs.length * Long.BYTES;
	}

}
