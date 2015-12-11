package de.uniduesseldorf.dxram.core.chunk.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.mem.ByteBufferDataStructureReaderWriter;
import de.uniduesseldorf.dxram.core.mem.Chunk;
import de.uniduesseldorf.dxram.core.mem.DataStructure;

import de.uniduesseldorf.menet.AbstractRequest;

/**
 * Request for updating a Chunk on a remote node
 * @author Florian Klein 09.03.2012
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public class PutRequest extends AbstractRequest {

	// DataStructure is used when sending the put request
	private DataStructure m_dataStructure = null;
	// Chunk is created and used when receiving a put request
	private Chunk m_chunk = null;
	private boolean m_releaseLock = false;

	/**
	 * Creates an instance of PutRequest.
	 * This constructor is used when receiving this message.
	 */
	public PutRequest() {
		super();
	}

	/**
	 * Creates an instance of PutRequest.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination
	 * @param p_dataStructure
	 *            Data structure with the data to put.
	 */
	public PutRequest(final short p_destination, final DataStructure p_dataStructure) {
		this(p_destination, p_dataStructure, false);
	}

	/**
	 * Creates an instance of PutRequest
	 * @param p_destination
	 *            the destination
	 * @param p_dataStructure
	 *            Data structure with the data to put.
	 * @param p_releaseLock
	 *            if true a potential lock will be released
	 */
	public PutRequest(final short p_destination, final DataStructure p_dataStructure, final boolean p_releaseLock) {
		super(p_destination, ChunkMessages.TYPE, ChunkMessages.SUBTYPE_PUT_REQUEST);

		m_dataStructure = p_dataStructure;
		m_releaseLock = p_releaseLock;
	}

	/**
	 * Get the Chunk to put
	 * @return the Chunk to put
	 */
	public final Chunk getChunk() {
		return m_chunk;
	}

	/**
	 * Checks if a potential lock should be released
	 * @return true if a potential lock should be released, false otherwise
	 */
	public final boolean isReleaseLock() {
		return m_releaseLock;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		ByteBufferDataStructureReaderWriter dataStructureWriter = new ByteBufferDataStructureReaderWriter(p_buffer);
		p_buffer.putLong(m_dataStructure.getID());
		m_dataStructure.writePayload(0, dataStructureWriter);
		p_buffer.put((byte) (m_releaseLock ? 1 : 0));
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_chunk = new Chunk(p_buffer.getLong());
		ByteBufferDataStructureReaderWriter dataStructureReader = new ByteBufferDataStructureReaderWriter(p_buffer);
		m_chunk.readPayload(0, dataStructureReader);
		m_releaseLock = p_buffer.get() != 0 ? true : false;
	}

	@Override
	protected final int getPayloadLengthForWrite() {		
		return Long.BYTES + m_dataStructure.sizeofPayload() + Byte.BYTES;
	}

}
