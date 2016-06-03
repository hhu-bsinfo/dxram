
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NodeID;

/**
 * Notify About New Predecessor Message
 * @author Kevin Beineke
 *         06.09.2012
 */
public class NotifyAboutNewPredecessorMessage extends AbstractMessage {

	// Attributes
	private short m_newPredecessor;

	// Constructors
	/**
	 * Creates an instance of NotifyAboutNewPredecessorMessage
	 */
	public NotifyAboutNewPredecessorMessage() {
		super();

		m_newPredecessor = -1;
	}

	/**
	 * Creates an instance of NotifyAboutNewPredecessorMessage
	 * @param p_destination
	 *            the destination
	 * @param p_newPredecessor
	 *            the new predecessor
	 */
	public NotifyAboutNewPredecessorMessage(final short p_destination, final short p_newPredecessor) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE);

		assert p_newPredecessor != NodeID.INVALID_ID;

		m_newPredecessor = p_newPredecessor;
	}

	// Getters
	/**
	 * Get the new predecessor
	 * @return the NodeID
	 */
	public final short getNewPredecessor() {
		return m_newPredecessor;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putShort(m_newPredecessor);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_newPredecessor = p_buffer.getShort();
	}

	@Override
	protected final int getPayloadLength() {
		return Short.BYTES;
	}

}
