package de.hhu.bsinfo.dxram.recovery.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Recover Message
 * @author Kevin Beineke
 *         12.10.2015
 */
public class RecoverMessage extends AbstractMessage {

	// Attributes
	private short m_owner;
	private boolean m_useLiveData;

	// Constructors
	/**
	 * Creates an instance of RecoverMessage
	 */
	public RecoverMessage() {
		super();

		m_owner = (short) -1;
	}

	/**
	 * Creates an instance of RecoverMessage
	 * @param p_destination
	 *            the destination
	 * @param p_useLiveData
	 *            whether the recover should use current logs or log files
	 * @param p_owner
	 *            the NodeID of the owner
	 */
	public RecoverMessage(final short p_destination, final short p_owner, final boolean p_useLiveData) {
		super(p_destination, RecoveryMessages.TYPE, RecoveryMessages.SUBTYPE_RECOVER_MESSAGE);

		m_owner = p_owner;
		m_useLiveData = p_useLiveData;
	}

	// Getters
	/**
	 * Get the owner
	 * @return the NodeID
	 */
	public final short getOwner() {
		return m_owner;
	}

	/**
	 * Returns whether the recover should use current logs or log files
	 * @return whether the recover should use current logs or log files
	 */
	public final boolean useLiveData() {
		return m_useLiveData;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putShort(m_owner);
		if (m_useLiveData) {
			p_buffer.put((byte) 1);
		} else {
			p_buffer.put((byte) 0);			
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_owner = p_buffer.getShort();
		
		byte b = p_buffer.get();
		if (b == 1) {
			m_useLiveData = true;
		}
	}

	@Override
	protected final int getPayloadLengthForWrite() {
		return Short.BYTES + Byte.BYTES;
	}

}
