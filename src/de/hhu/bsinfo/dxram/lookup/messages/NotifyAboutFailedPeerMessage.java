package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.util.NodeID;
import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Notify About Failed Peer Message
 * @author Kevin Beineke
 *         06.09.2012
 */
public class NotifyAboutFailedPeerMessage extends AbstractMessage {

	// Attributes
	private short m_failedPeer;

	// Constructors
	/**
	 * Creates an instance of NotifyAboutFailedPeerMessage
	 */
	public NotifyAboutFailedPeerMessage() {
		super();

		m_failedPeer = -1;
	}

	/**
	 * Creates an instance of NotifyAboutFailedPeerMessage
	 * @param p_destination
	 *            the destination
	 * @param p_failedPeer
	 *            the failed peer
	 */
	public NotifyAboutFailedPeerMessage(final short p_destination, final short p_failedPeer) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_MESSAGE);

		assert p_failedPeer != NodeID.INVALID_ID;

		m_failedPeer = p_failedPeer;
	}

	// Getters
	/**
	 * Get the failed peer
	 * @return the NodeID
	 */
	public final short getFailedPeer() {
		return m_failedPeer;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putShort(m_failedPeer);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_failedPeer = p_buffer.getShort();
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Short.BYTES;
	}

}
