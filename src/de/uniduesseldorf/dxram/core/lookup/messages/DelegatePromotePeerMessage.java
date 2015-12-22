package de.uniduesseldorf.dxram.core.lookup.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.menet.AbstractMessage;

/**
 * Delegate Promote Peer Message
 * @author Kevin Beineke
 *         06.09.2012
 */
public class DelegatePromotePeerMessage extends AbstractMessage {

	// Attributes
	private short m_hops;

	// Constructors
	/**
	 * Creates an instance of DelegatePromotePeerMessage
	 */
	public DelegatePromotePeerMessage() {
		super();

		m_hops = -1;
	}

	/**
	 * Creates an instance of DelegatePromotePeerMessage
	 * @param p_destination
	 *            the destination
	 * @param p_hops
	 *            the number of hops until now
	 */
	public DelegatePromotePeerMessage(final short p_destination, final short p_hops) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_DELEGATE_PROMOTE_PEER_MESSAGE);

		m_hops = p_hops;
	}

	// Getters
	/**
	 * Get hops
	 * @return the number of hops
	 */
	public final short getHops() {
		return m_hops;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putShort(m_hops);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_hops = p_buffer.getShort();
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Short.BYTES;
	}

}
