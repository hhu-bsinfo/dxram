
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Init Range Request
 * @author Kevin Beineke
 *         03.06.2013
 */
public class InitRangeRequest extends AbstractRequest {

	// Attributes
	private long m_startChunkIDOrRangeID;
	private long m_lookupRange;
	private boolean m_isBackup;

	// Constructors
	/**
	 * Creates an instance of InitRangeRequest
	 */
	public InitRangeRequest() {
		super();

		m_startChunkIDOrRangeID = -1;
		m_lookupRange = -1;
		m_isBackup = false;
	}

	/**
	 * Creates an instance of InitRangeRequest
	 * @param p_destination
	 *            the destination
	 * @param p_startChunkID
	 *            the first object
	 * @param p_lookupRange
	 *            the LookupRange (backup peers and own NodeID)
	 * @param p_isBackup
	 *            whether this is a backup message or not
	 */
	public InitRangeRequest(final short p_destination, final long p_startChunkID, final long p_lookupRange, final boolean p_isBackup) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_INIT_RANGE_REQUEST);

		m_startChunkIDOrRangeID = p_startChunkID;
		m_lookupRange = p_lookupRange;
		m_isBackup = p_isBackup;
	}

	// Getters
	/**
	 * Get the last ChunkID
	 * @return the ID
	 */
	public final long getStartChunkIDOrRangeID() {
		return m_startChunkIDOrRangeID;
	}

	/**
	 * Get lookupRange
	 * @return the LookupRange
	 */
	public final long getLookupRange() {
		return m_lookupRange;
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
		p_buffer.putLong(m_startChunkIDOrRangeID);
		p_buffer.putLong(m_lookupRange);
		if (m_isBackup) {
			p_buffer.put((byte) 1);
		} else {
			p_buffer.put((byte) 0);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_startChunkIDOrRangeID = p_buffer.getLong();
		m_lookupRange = p_buffer.getLong();

		final byte b = p_buffer.get();
		if (b == 1) {
			m_isBackup = true;
		}
	}

	@Override
	protected final int getPayloadLength() {
		return Long.BYTES * 2 + Byte.BYTES;
	}

}
