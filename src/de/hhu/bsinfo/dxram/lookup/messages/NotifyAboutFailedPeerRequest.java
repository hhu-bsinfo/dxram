
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.menet.AbstractRequest;
import de.hhu.bsinfo.menet.NodeID;

/**
 * Notify About Failed Peer Request
 * @author Kevin Beineke
 *         06.09.2012
 */
public class NotifyAboutFailedPeerRequest extends AbstractRequest {

	// Attributes
	private short m_failedPeer;

	// Constructors
	/**
	 * Creates an instance of NotifyAboutFailedPeerRequest
	 */
	public NotifyAboutFailedPeerRequest() {
		super();

		m_failedPeer = -1;
	}

	/**
	 * Creates an instance of NotifyAboutFailedPeerRequest
	 * @param p_destination
	 *            the destination
	 * @param p_failedPeer
	 *            the failed peer
	 */
	public NotifyAboutFailedPeerRequest(final short p_destination, final short p_failedPeer) {
		super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
				LookupMessages.SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_REQUEST);

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
	protected final int getPayloadLength() {
		return Short.BYTES;
	}

}
