package de.uniduesseldorf.dxram.core.lookup.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.menet.AbstractRequest;

/**
 * Migrate Range Request
 * @author Kevin Beineke
 *         03.06.2013
 */
public class MigrateRangeRequest extends AbstractRequest {

	// Attributes
	private long m_startChunkID;
	private long m_endChunkID;
	private short m_nodeID;
	private boolean m_isBackup;

	// Constructors
	/**
	 * Creates an instance of MigrateRangeRequest
	 */
	public MigrateRangeRequest() {
		super();

		m_startChunkID = -1;
		m_endChunkID = -1;
		m_nodeID = -1;
		m_isBackup = false;
	}

	/**
	 * Creates an instance of MigrateRangeRequest
	 * @param p_destination
	 *            the destination
	 * @param p_startChunkID
	 *            the first object that has to be migrated
	 * @param p_endChunkID
	 *            the last object that has to be migrated
	 * @param p_nodeID
	 *            the peer where the object has to be migrated
	 * @param p_isBackup
	 *            whether this is a backup message or not
	 */
	public MigrateRangeRequest(final short p_destination, final long p_startChunkID,
			final long p_endChunkID, final short p_nodeID, final boolean p_isBackup) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_MIGRATE_RANGE_REQUEST);

		m_startChunkID = p_startChunkID;
		m_endChunkID = p_endChunkID;
		m_nodeID = p_nodeID;
		m_isBackup = p_isBackup;
	}

	// Getters
	/**
	 * Get the first ChunkID
	 * @return the ID
	 */
	public final long getStartChunkID() {
		return m_startChunkID;
	}

	/**
	 * Get the last ChunkID
	 * @return the ID
	 */
	public final long getEndChunkID() {
		return m_endChunkID;
	}

	/**
	 * Get the NodeID
	 * @return the NodeID
	 */
	public final short getNodeID() {
		return m_nodeID;
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
		p_buffer.putLong(m_startChunkID);
		p_buffer.putLong(m_endChunkID);
		p_buffer.putShort(m_nodeID);
		p_buffer.put((byte) (m_isBackup ? 1 : 0));
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_startChunkID = p_buffer.getLong();
		m_endChunkID = p_buffer.getLong();
		m_nodeID = p_buffer.getShort();
		m_isBackup = p_buffer.get() != 0 ? true : false;
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Long.BYTES * 2 + Short.BYTES + Byte.BYTES;
	}

}
