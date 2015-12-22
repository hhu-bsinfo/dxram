package de.uniduesseldorf.dxram.core.lookup.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.menet.AbstractMessage;

/**
 * Update All Message
 * @author Kevin Beineke
 *         12.10.2015
 */
public class UpdateAllMessage extends AbstractMessage {

	// Attributes
	private short m_owner;

	// Constructors
	/**
	 * Creates an instance of UpdateAllMessage
	 */
	public UpdateAllMessage() {
		super();

		m_owner = -1;
	}

	/**
	 * Creates an instance of UpdateAllMessage
	 * @param p_destination
	 *            the destination
	 * @param p_owner
	 *            the failed peer
	 */
	public UpdateAllMessage(final short p_source, final short p_destination, final short p_owner) {
		super(p_source, p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_UPDATE_ALL_MESSAGE);

		m_owner = p_owner;
	}

	// Getters
	/**
	 * Get the owner
	 * @return the NodeID
	 */
	public final short getOwner() {
		return m_owner;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putShort(m_owner);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_owner = p_buffer.getShort();
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Short.BYTES;
	}

}
