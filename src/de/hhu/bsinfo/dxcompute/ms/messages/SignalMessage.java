package de.hhu.bsinfo.dxcompute.ms.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxcompute.ms.Signal;
import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Message to send signal codes from master to slave or vice versa
 * to abort execution for example
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class SignalMessage extends AbstractMessage {
	private Signal m_signal;

	/**
	 * Creates an instance of SignalMessage.
	 * This constructor is used when receiving this message.
	 */
	public SignalMessage() {
		super();
	}

	/**
	 * Creates an instance of SignalMessage.
	 * This constructor is used when sending this message.
	 *
	 * @param p_destination the destination node id.
	 * @param p_signal      signal to send
	 */
	public SignalMessage(final short p_destination, final Signal p_signal) {
		super(p_destination, MasterSlaveMessages.TYPE, MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_FINISHED_MESSAGE);

		m_signal = p_signal;
	}

	/**
	 * Get the signal triggered
	 *
	 * @return Signal
	 */
	public Signal getSignal() {
		return m_signal;
	}

	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_signal.ordinal());
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_signal = Signal.values()[p_buffer.getInt()];
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES;
	}
}
