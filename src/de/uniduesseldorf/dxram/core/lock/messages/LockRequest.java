package de.uniduesseldorf.dxram.core.lock.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.mem.DataStructure;
import de.uniduesseldorf.dxram.core.util.ChunkID;

import de.uniduesseldorf.menet.AbstractRequest;

/**
 * Request for locking a Chunk on a remote node
 * @author Florian Klein 09.03.2012
 */
public class LockRequest extends AbstractRequest {

	// Attributes
	// keep data structure, so lock response can write data directly to it
	private DataStructure m_dataStructure = null;
	private long m_chunkID = ChunkID.INVALID_ID;
	private boolean m_readLock = false;

	// Constructors
	/**
	 * Creates an instance of LockRequest
	 */
	public LockRequest() {
		super();
	}

	/**
	 * Creates an instance of LockRequest
	 * @param p_destination
	 *            the destination
	 * @param p_chunkID
	 *            The ID of the Chunk to lock
	 */
	public LockRequest(final short p_destination, final DataStructure p_dataStructure) {
		this(p_destination, p_dataStructure, false);
	}

	/**
	 * Creates an instance of LockRequest
	 * @param p_destination
	 *            the destination
	 * @param p_chunkID
	 *            The ID of the Chunk to lock
	 * @param p_readLock
	 *            if true the lock is a read lock, otherwise the lock is a write lock
	 */
	public LockRequest(final short p_destination, final DataStructure p_dataStructure, final boolean p_readLock) {
		super(p_destination, LockMessages.TYPE, LockMessages.SUBTYPE_LOCK_REQUEST);

		m_dataStructure = p_dataStructure;
		m_chunkID = m_dataStructure.getID();
		m_readLock = p_readLock;
	}

	// Getters
	/**
	 * Get the ID of the Chunk to lock
	 * @return the ID of the Chunk to lock
	 */
	public final long getChunkID() {
		return m_chunkID;
	}

	/**
	 * Checks if the lock is a read lock
	 * @return true if the lock is a read lock, false otherwise
	 */
	public final boolean isReadLock() {
		return m_readLock;
	}
	
	DataStructure getDataStructure()
	{
		return m_dataStructure;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putLong(m_chunkID);
		p_buffer.put((byte) (m_readLock ? 1 : 0));
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_chunkID = p_buffer.getLong();
		m_readLock = p_buffer.get() != 0 ? true : false;
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Long.BYTES + Byte.BYTES;
	}

}
