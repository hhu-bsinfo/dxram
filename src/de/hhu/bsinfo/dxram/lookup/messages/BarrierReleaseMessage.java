
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Message to release the signed on instances from the barrier.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.05.16
 */
public class BarrierReleaseMessage extends AbstractMessage {
	private int m_barrierId = -1;
	private short[] m_signedOnPeers;
	private long[] m_customData;

	/**
	 * Creates an instance of BarrierReleaseMessage.
	 * This constructor is used when receiving this message.
	 */
	public BarrierReleaseMessage() {
		super();
	}

	/**
	 * Creates an instance of BarrierReleaseMessage.
	 * This constructor is used when sending this message.
	 * @param p_destination
	 *            the destination node id.
	 * @param p_barrierId
	 *            Id of the barrier that got released
	 * @param p_signedOnPeers
	 *            List of peers that signed on for the barrier
	 * @param p_customData
	 *            Custom data (ordered by signed on list) of the peers
	 */
	public BarrierReleaseMessage(final short p_destination, final int p_barrierId, final short[] p_signedOnPeers,
			final long[] p_customData) {
		super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_RELEASE_MESSAGE);

		m_barrierId = p_barrierId;
		// the first entry of the list contains the number of signed on peers from the BarrierTable
		// this is not needed (anymore) here and will be dropped on the serialization calls
		m_signedOnPeers = p_signedOnPeers;
		m_customData = p_customData;
	}

	/**
	 * Get the id of the barrier that got released
	 * @return Barrier id.
	 */
	public int getBarrierId() {
		return m_barrierId;
	}

	/**
	 * Get the peers that signed on.
	 * @return List of peers.
	 */
	public short[] getSignedOnPeers() {
		return m_signedOnPeers;
	}

	/**
	 * Get custom data the peers provided with the sign on.
	 * @return Custom data of the peers.
	 */
	public long[] getCustomData() {
		return m_customData;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_barrierId);
		// drop the first index of the peer list, which is the number of signed on peers (obsolete here)
		p_buffer.putInt(m_signedOnPeers.length - 1);
		for (int i = 1; i < m_signedOnPeers.length; i++) {
			p_buffer.putShort(m_signedOnPeers[i]);
		}
		p_buffer.putInt(m_customData.length);
		for (int i = 0; i < m_customData.length; i++) {
			p_buffer.putLong(m_customData[i]);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_barrierId = p_buffer.getInt();

		m_signedOnPeers = new short[p_buffer.getInt()];
		for (int i = 0; i < m_signedOnPeers.length; i++) {
			m_signedOnPeers[i] = p_buffer.getShort();
		}

		m_customData = new long[p_buffer.getInt()];
		for (int i = 0; i < m_customData.length; i++) {
			m_customData[i] = p_buffer.getLong();
		}
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES + Integer.BYTES + Short.BYTES * (m_signedOnPeers.length - 1) + Integer.BYTES
				+ Long.BYTES * m_customData.length;
	}
}
