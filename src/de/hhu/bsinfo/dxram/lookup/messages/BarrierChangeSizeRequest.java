
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractRequest;

/**
 * Change the size of an existing barrier.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 06.05.16
 */
public class BarrierChangeSizeRequest extends AbstractRequest {
	private int m_barrierId;
	private int m_size;

	/**
	 * Creates an instance of BarrierChangeSizeRequest
	 */
	public BarrierChangeSizeRequest() {
		super();
	}

	/**
	 * Creates an instance of BarrierChangeSizeRequest
	 * @param p_destination
	 *            the destination
	 * @param p_size
	 *            size of the barrier
	 */
	public BarrierChangeSizeRequest(final short p_destination, final int p_barrierId, final int p_size) {
		super(p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_BARRIER_CHANGE_SIZE_REQUEST);

		m_barrierId = p_barrierId;
		m_size = p_size;
	}

	/**
	 * Id of the barrier to change the size of.
	 * @return Barrier id
	 */
	public int getBarrierId() {
		return m_barrierId;
	}

	/**
	 * Get the barrier size;
	 * @return Barrier size
	 */
	public int getBarrierSize() {
		return m_size;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_barrierId);
		p_buffer.putInt(m_size);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_barrierId = p_buffer.getInt();
		m_size = p_buffer.getInt();
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES + Integer.BYTES;
	}
}
