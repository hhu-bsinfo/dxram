
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Notify About New Successor Message
 * @author Kevin Beineke
 *         06.09.2012
 */
public class NotifyAboutNewSuccessorMessage extends AbstractMessage {

	// Attributes
	private short m_newSuccessor;

	// Constructors
	/**
	 * Creates an instance of NotifyAboutNewSuccessorMessage
	 */
	public NotifyAboutNewSuccessorMessage() {
		super();

		m_newSuccessor = -1;
	}

	/**
	 * Creates an instance of NotifyAboutNewSuccessorMessage
	 * @param p_destination
	 *            the destination
	 * @param p_newSuccessor
	 *            the new successor
	 */
	public NotifyAboutNewSuccessorMessage(final short p_destination, final short p_newSuccessor) {
		super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
				LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE);

		assert p_newSuccessor != NodeID.INVALID_ID;

		m_newSuccessor = p_newSuccessor;
	}

	// Getters
	/**
	 * Get new successor
	 * @return the NodeID
	 */
	public final short getNewSuccessor() {
		return m_newSuccessor;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putShort(m_newSuccessor);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_newSuccessor = p_buffer.getShort();
	}

	@Override
	protected final int getPayloadLength() {
		return Short.BYTES;
	}

}
