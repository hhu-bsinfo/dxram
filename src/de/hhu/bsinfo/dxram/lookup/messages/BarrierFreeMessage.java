package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.menet.AbstractMessage;

import java.nio.ByteBuffer;

/**
 * Message to free an allocated barrier.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.05.16
 */
public class BarrierFreeMessage extends AbstractMessage {
	private int m_barrierId;

	/**
	 * Creates an instance of BarrierFreeMessage
	 */
	public BarrierFreeMessage() {
		super();
	}

	/**
	 * Creates an instance of BarrierFreeMessage
	 *
	 * @param p_destination the destination
	 * @param p_barrierId   Id of the barrier to free
	 */
	public BarrierFreeMessage(final short p_destination, final int p_barrierId) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_BARRIER_FREE_MESSAGE);

		m_barrierId = p_barrierId;
	}

	/**
	 * Get the barrier id to free.
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
