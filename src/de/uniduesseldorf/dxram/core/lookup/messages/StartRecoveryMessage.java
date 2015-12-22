package de.uniduesseldorf.dxram.core.lookup.messages;

import java.nio.ByteBuffer;

import de.uniduesseldorf.menet.AbstractMessage;
import de.uniduesseldorf.utils.Contract;

/**
 * Start Recovery Message
 * @author Kevin Beineke
 *         06.09.2012
 */
public class StartRecoveryMessage extends AbstractMessage {

	// Attributes
	private short m_failedPeer;
	private long m_beginOfRange;

	// Constructors
	/**
	 * Creates an instance of StartRecoveryMessage
	 */
	public StartRecoveryMessage() {
		super();

		m_failedPeer = -1;
		m_beginOfRange = -1;
	}

	/**
	 * Creates an instance of StartRecoveryMessage
	 * @param p_destination
	 *            the destination
	 * @param p_failedPeer
	 *            the failed peer
	 * @param p_beginOfRange
	 *            the beginning of the range that has to be recovered
	 */
	public StartRecoveryMessage(final short p_source, final short p_destination, final short p_failedPeer, final int p_beginOfRange) {
		super(p_source, p_destination, LookupMessages.TYPE, LookupMessages.SUBTYPE_START_RECOVERY_MESSAGE);

		Contract.checkNotNull(p_failedPeer, "no failed peer given");

		m_failedPeer = p_failedPeer;
		m_beginOfRange = p_beginOfRange;
	}

	// Getters
	/**
	 * Get the failed peer
	 * @return the NodeID
	 */
	public final short getFailedPeer() {
		return m_failedPeer;
	}

	/**
	 * Get the beginning of range
	 * @return the beginning of the range that has to be recovered
	 */
	public final long getBeginOfRange() {
		return m_beginOfRange;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putShort(m_failedPeer);
		p_buffer.putLong(m_beginOfRange);
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_failedPeer = p_buffer.getShort();
		m_beginOfRange = p_buffer.getLong();
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Short.BYTES + Long.BYTES;
	}

}
