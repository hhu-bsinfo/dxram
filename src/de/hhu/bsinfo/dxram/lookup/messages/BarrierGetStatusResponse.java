package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.menet.AbstractResponse;

import java.nio.ByteBuffer;

/**
 * Message to get the current status of an active barrier.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 06.05.16
 */
public class BarrierGetStatusResponse extends AbstractResponse {
	private int m_barrierId;
	private short[] m_barrierStatus = new short[0];

	/**
	 * Creates an instance of BarrierGetStatusResponse.
	 * This constructor is used when receiving this message.
	 */
	public BarrierGetStatusResponse() {
		super();
	}

	/**
	 * Creates an instance of BarrierGetStatusResponse.
	 * This constructor is used when sending this message.
	 *
	 * @param p_request       The request for the response
	 * @param p_barrierStatus Status of the barrier
	 */
	public BarrierGetStatusResponse(final BarrierGetStatusRequest p_request, short[] p_barrierStatus) {
		super(p_request, LookupMessages.SUBTYPE_BARRIER_STATUS_RESPONSE);

		m_barrierId = p_request.getBarrierId();
		m_barrierStatus = p_barrierStatus;
	}

	/**
	 * Get the id of the barrier
	 *
	 * @return Barrier id.
	 */
	public int getBarrierId() {
		return m_barrierId;
	}

	/**
	 * Get the barrier status.
	 * First value is the number of signed on peers.
	 *
	 * @return Barrier status.
	 */
	public short[] getBarrierStatus() {
		return m_barrierStatus;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_barrierId);
		p_buffer.putInt(m_barrierStatus.length);
		for (int i = 0; i < m_barrierStatus.length; i++) {
			p_buffer.putShort(m_barrierStatus[i]);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_barrierId = p_buffer.getInt();
		m_barrierStatus = new short[p_buffer.getInt()];
		for (int i = 0; i < m_barrierStatus.length; i++) {
			m_barrierStatus[i] = p_buffer.getShort();
		}
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES + Integer.BYTES + m_barrierStatus.length * Short.BYTES;
	}
}
