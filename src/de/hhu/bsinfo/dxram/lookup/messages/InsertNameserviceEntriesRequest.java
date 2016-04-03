
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Request for storing an id to ChunkID mapping on a remote node
 * @author Florian Klein
 *         09.03.2012
 */
public class InsertNameserviceEntriesRequest extends AbstractRequest {

	// Attributes
	private int m_id;
	private long m_chunkID;
	private boolean m_isBackup;

	// Constructors
	/**
	 * Creates an instance of InsertIDRequest
	 */
	public InsertNameserviceEntriesRequest() {
		super();

		m_id = -1;
		m_chunkID = ChunkID.INVALID_ID;
		m_isBackup = false;
	}

	/**
	 * Creates an instance of InsertIDRequest
	 * @param p_destination
	 *            the destination
	 * @param p_id
	 *            the id to store
	 * @param p_chunkID
	 *            the ChunkID to store
	 * @param p_isBackup
	 *            whether this is a backup message or not
	 */
	public InsertNameserviceEntriesRequest(final short p_destination, final int p_id, final long p_chunkID, final boolean p_isBackup) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_INSERT_NAMESERVICE_ENTRIES_REQUEST);

		m_id = p_id;
		m_chunkID = p_chunkID;
		m_isBackup = p_isBackup;
	}

	// Getters
	/**
	 * Get the id to store
	 * @return the id to store
	 */
	public final int getID() {
		return m_id;
	}

	/**
	 * Get the ChunkID to store
	 * @return the ChunkID to store
	 */
	public final long getChunkID() {
		return m_chunkID;
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
		p_buffer.putInt(m_id);
		p_buffer.putLong(m_chunkID);
		if (m_isBackup) {
			p_buffer.put((byte) 1);
		} else {
			p_buffer.put((byte) 0);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_id = p_buffer.getInt();
		m_chunkID = p_buffer.getLong();

		final byte b = p_buffer.get();
		if (b == 1) {
			m_isBackup = true;
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Integer.BYTES + Long.BYTES + Byte.BYTES;
	}

}
