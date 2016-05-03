package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

import java.nio.ByteBuffer;

/**
 * Message to release the signed on instances from the barrier.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.05.16
 */
public class BarrierReleaseMessage extends AbstractMessage {
	private int m_barrierId = -1;

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
	 *
	 * @param p_destination the destination node id.
	 * @param p_barrierId   Id of the barrier that got released
	 */
	public BarrierReleaseMessage(final short p_destination, final int p_barrierId) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_BARRIER_RELEASE_MESSAGE);

		m_barrierId = p_barrierId;
	}

	/**
	 * Get the id of the barrier that got released
	 *
	 * @return Barrier id.
	 */
	public int getBarrierId() {
		return m_barrierId;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_barrierId);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_barrierId = p_buffer.getInt();
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES;
	}
}
