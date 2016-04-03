package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Get Backup Ranges Request
 * @author Kevin Beineke
 *         08.10.2015
 */
public class GetAllBackupRangesRequest extends AbstractRequest {

	// Attributes
	private short m_nodeID;

	// Constructors
	/**
	 * Creates an instance of GetBackupRangesRequest
	 */
	public GetAllBackupRangesRequest() {
		super();

		m_nodeID = -1;
	}

	/**
	 * Creates an instance of GetBackupRangesRequest
	 * @param p_destination
	 *            the destination
	 * @param p_nodeID
	 *            the NodeID
	 */
	public GetAllBackupRangesRequest(final short p_destination, final short p_nodeID) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_ALL_BACKUP_RANGES_REQUEST);

		m_nodeID = p_nodeID;
	}

	// Getters
	/**
	 * Get the NodeID
	 * @return the NodeID
	 */
	public final short getNodeID() {
		return m_nodeID;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putShort(m_nodeID);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_nodeID = p_buffer.getShort();
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Short.BYTES;
	}

}
