package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.menet.AbstractRequest;

import java.nio.ByteBuffer;

/**
 * Request to allocate/create a new barrier.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.05.16
 */
public class BarrierAllocRequest extends AbstractRequest {
	private int m_size;

	/**
	 * Creates an instance of BarrierAllocRequest
	 */
	public BarrierAllocRequest() {
		super();
	}

	/**
	 * Creates an instance of LookupRequest
	 *
	 * @param p_destination the destination
	 * @param p_size        size of the barrier
	 */
	public BarrierAllocRequest(final short p_destination, final int p_size) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_BARRIER_ALLOC_REQUEST);

		m_size = p_size;
	}

	/**
	 * Get the barrier size;
	 *
	 * @return Barrier size
	 */
	public int getBarrierSize() {
		return m_size;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_size);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_size = p_buffer.getInt();
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES;
	}
}
