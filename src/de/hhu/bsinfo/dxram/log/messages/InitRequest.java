
package de.hhu.bsinfo.dxram.log.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Request for initialization of a backup range on a remote node
 * @author Kevin Beineke 20.04.2014
 */
public class InitRequest extends AbstractRequest {

	// Attributes
	private long m_firstChunkIDOrRangeID;
	private short m_owner;

	// Constructors
	/**
	 * Creates an instance of InitRequest
	 */
	public InitRequest() {
		super();

		m_firstChunkIDOrRangeID = 0;
		m_owner = -1;
	}

	/**
	 * Creates an instance of InitRequest
	 * @param p_destination
	 *            the destination
	 * @param p_firstChunkIDOrRangeID
	 *            the beginning of the range
	 * @param p_owner
	 *            the current owner
	 */
	public InitRequest(final short p_destination, final long p_firstChunkIDOrRangeID, final short p_owner) {
		super(p_destination, LogMessages.TYPE, LogMessages.SUBTYPE_INIT_REQUEST, true);

		m_firstChunkIDOrRangeID = p_firstChunkIDOrRangeID;
		m_owner = p_owner;
	}

	// Getters
	/**
	 * Get the beginning of the range
	 * @return the ChunkID
	 */
	public final long getFirstCIDOrRangeID() {
		return m_firstChunkIDOrRangeID;
	}

	/**
	 * Get the current owner
	 * @return the current owner
	 */
	public final short getOwner() {
		return m_owner;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putLong(m_firstChunkIDOrRangeID);
		p_buffer.putShort(m_owner);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_firstChunkIDOrRangeID = p_buffer.getLong();
		m_owner = p_buffer.getShort();
	}

	@Override
	protected final int getPayloadLength() {
		return Long.BYTES + Short.BYTES;
	}
}
