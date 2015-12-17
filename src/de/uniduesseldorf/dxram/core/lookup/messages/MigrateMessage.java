package de.uniduesseldorf.dxram.core.lookup.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.menet.AbstractMessage;

/**
 * Migrate Message
 * @author Kevin Beineke
 *         03.06.2013
 */
public class MigrateMessage extends AbstractMessage {

	// Attributes
	private long m_chunkID;
	private short m_nodeID;
	private boolean m_isBackup;

	// Constructors
	/**
	 * Creates an instance of MigrateMessage
	 */
	public MigrateMessage() {
		super();

		m_chunkID = -1;
		m_nodeID = -1;
		m_isBackup = false;
	}

	/**
	 * Creates an instance of MigrateMessage
	 * @param p_destination
	 *            the destination
	 * @param p_chunkID
	 *            the object that has to be migrated
	 * @param p_nodeID
	 *            the peer where the object has to be migrated
	 * @param p_isBackup
	 *            whether this is a backup message or not
	 */
	public MigrateMessage(final short p_destination, final long p_chunkID, final short p_nodeID, final boolean p_isBackup) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_MIGRATE_MESSAGE);

		m_chunkID = p_chunkID;
		m_nodeID = p_nodeID;
		m_isBackup = p_isBackup;
	}

	// Getters
	/**
	 * Get the ChunkID
	 * @return the ID
	 */
	public final long getChunkID() {
		return m_chunkID;
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
		p_buffer.putLong(m_chunkID);
		p_buffer.putShort(m_nodeID);
		p_buffer.put((byte) (m_isBackup ? 1 : 0));
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_chunkID = p_buffer.getLong();
		m_nodeID = p_buffer.getShort();
		m_isBackup = p_buffer.get() != 0 ? true : false;
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Long.BYTES + Short.BYTES + Byte.BYTES;
	}

}
