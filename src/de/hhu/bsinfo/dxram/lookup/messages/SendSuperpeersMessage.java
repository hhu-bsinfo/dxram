
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Send Superpeers Message
 * @author Kevin Beineke
 *         06.09.2012
 */
public class SendSuperpeersMessage extends AbstractMessage {

	// Attributes
	private ArrayList<Short> m_superpeers;

	// Constructors
	/**
	 * Creates an instance of SendSuperpeersMessage
	 */
	public SendSuperpeersMessage() {
		super();

		m_superpeers = null;
	}

	/**
	 * Creates an instance of SendSuperpeersMessage
	 * @param p_destination
	 *            the destination
	 * @param p_superpeers
	 *            the superpeers
	 */
	public SendSuperpeersMessage(final short p_destination, final ArrayList<Short> p_superpeers) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE);

		assert p_superpeers != null;

		m_superpeers = p_superpeers;
	}

	// Getters
	/**
	 * Get the superpeers
	 * @return the superpeer array
	 */
	public final ArrayList<Short> getSuperpeers() {
		return m_superpeers;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		if (m_superpeers == null || m_superpeers.size() == 0) {
			p_buffer.putInt(0);
		} else {
			p_buffer.putInt(m_superpeers.size());
			for (short superpeer : m_superpeers) {
				p_buffer.putShort(superpeer);
			}
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int length;

		m_superpeers = new ArrayList<Short>();
		length = p_buffer.getInt();
		for (int i = 0; i < length; i++) {
			m_superpeers.add(p_buffer.getShort());
		}
	}

	@Override
	protected final int getPayloadLength() {
		int ret;

		ret = Integer.BYTES;
		if (m_superpeers != null && m_superpeers.size() > 0) {
			ret += Short.BYTES * m_superpeers.size();
		}

		return ret;
	}

}
