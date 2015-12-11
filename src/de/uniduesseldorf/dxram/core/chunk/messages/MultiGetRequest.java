package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.mem.DataStructure;

import de.uniduesseldorf.menet.AbstractRequest;

/**
 * Request for getting multiple Chunks from a remote node
 * @author Florian Klein 05.07.2014
 */
public class MultiGetRequest extends AbstractRequest {

	// Attributes
	// data structures affected by the request are stored, so the
	// response can write directly to them
	private DataStructure[] m_dataStructures = null;
	// this is only used when receiving the request
	private long[] m_chunkIDs = null;

	// Constructors
	/**
	 * Creates an instance of MultiGetRequest
	 */
	public MultiGetRequest() {
		super();
	}

	/**
	 * Creates an instance of MultiGetRequest
	 * @param p_destination
	 *            the destination
	 * @param p_chunkIDs
	 *            The IDs of the Chunk to get
	 */
	public MultiGetRequest(final short p_destination, final DataStructure[] p_dataStructures) {
		super(p_destination, ChunkMessages.TYPE, ChunkMessages.SUBTYPE_MULTIGET_REQUEST);
		m_dataStructures = p_dataStructures;
		
		m_chunkIDs = new long[m_dataStructures.length];
		for (int i = 0; i < m_chunkIDs.length; i++) {
			m_chunkIDs[i] = m_dataStructures[i].getID();
		}
	}

	// Getters
	/**
	 * Get the IDs of the Chunks to get
	 * @return the IDs of the Chunks to get
	 */
	public final long[] getChunkIDs() {
		return m_chunkIDs;
	}
	
	DataStructure[] getDataStructures()
	{
		return m_dataStructures;
	}

	// Methods
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
