package de.hhu.bsinfo.menet;

import java.nio.ByteBuffer;

/**
 * Used to confirm received bytes
 * @author Marc Ewert 14.10.2014
 */
public final class FlowControlMessage extends AbstractMessage {

	public static final byte TYPE = 0;
	public static final byte SUBTYPE = 1;

	private int m_confirmedBytes;

	/**
	 * Default constructor for serialization
	 */
	FlowControlMessage() {}

	/**
	 * Create a new Message for confirming received bytes.
	 * @param p_confirmedBytes
	 *            number of received bytes
	 */
	FlowControlMessage(final int p_confirmedBytes) {
		super((short) 0, TYPE, SUBTYPE, true);
		m_confirmedBytes = p_confirmedBytes;
	}

	/**
	 * Get number of confirmed bytes
	 * @return
	 *         the number of confirmed bytes
	 */
	public int getConfirmedBytes() {
		return m_confirmedBytes;
	}

	@Override
	protected void readPayload(final ByteBuffer p_buffer) {
		m_confirmedBytes = p_buffer.getInt();
	}

	@Override
	protected void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_confirmedBytes);
	}

	@Override
	protected int getPayloadLengthForWrite() {
		return 4;
	}
}