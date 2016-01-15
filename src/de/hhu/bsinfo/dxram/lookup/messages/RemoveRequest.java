package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractRequest;
import de.hhu.bsinfo.utils.Contract;

/**
 * Remove Request
 * @author Kevin Beineke
 *         06.09.2012
 */
public class RemoveRequest extends AbstractRequest {

	// Attributes
	private long[] m_chunkIDs;
	private boolean m_isBackup;

	// Constructors
	/**
	 * Creates an instance of RemoveRequest
	 */
	public RemoveRequest() {
		super();

		m_chunkIDs = null;
		m_isBackup = false;
	}

	/**
	 * Creates an instance of RemoveRequest
	 * @param p_destination
	 *            the destination
	 * @param p_chunkIDs
	 *            the ChunkIDs that have to be removed
	 * @param p_isBackup
	 *            whether this is a backup message or not
	 */
	public RemoveRequest(final short p_destination, final long[] p_chunkIDs, final boolean p_isBackup) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_REMOVE_REQUEST);

		Contract.checkNotNull(p_chunkIDs, "no ChunkIDs given");

		m_chunkIDs = p_chunkIDs;
		m_isBackup = p_isBackup;
	}

	// Getters
	/**
	 * Get the ChunkID
	 * @return the ChunkID
	 */
	public final long[] getChunkIDs() {
		return m_chunkIDs;
	}

	/**
	 * Returns whether this is a backup message or not
	 * @return whether this is a backup message or not
	 */
	public final boolean isBackup() {
		return m_isBackup;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_chunkIDs.length);
		p_buffer.asLongBuffer().put(m_chunkIDs);
		p_buffer.position(p_buffer.position() + m_chunkIDs.length * Long.BYTES);
		p_buffer.put((byte) (m_isBackup ? 1 : 0));
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_chunkIDs = new long[p_buffer.getInt()];
		p_buffer.asLongBuffer().get(m_chunkIDs);
		p_buffer.position(p_buffer.position() + m_chunkIDs.length * Long.BYTES);
		m_isBackup = p_buffer.get() != 0 ? true : false;			
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Integer.BYTES + Long.BYTES * m_chunkIDs.length + Byte.BYTES;
	}

}
