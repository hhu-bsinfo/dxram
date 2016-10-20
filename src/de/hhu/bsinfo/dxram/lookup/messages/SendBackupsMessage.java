
package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.menet.AbstractMessage;

/**
 * Send Backups Message
 * @author Kevin Beineke
 *         06.09.2012
 */
public class SendBackupsMessage extends AbstractMessage {

	// Attributes
	private byte[] m_metadata;

	// Constructors
	/**
	 * Creates an instance of SendBackupsMessage
	 */
	public SendBackupsMessage() {
		super();

		m_metadata = null;
	}

	/**
	 * Creates an instance of SendBackupsMessage
	 * @param p_destination
	 *            the destination
	 * @param p_metadata
	 *            the metadata
	 */
	public SendBackupsMessage(final short p_destination, final byte[] p_metadata) {
		super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE);

		m_metadata = p_metadata;
	}

	// Getters
	/**
	 * Get metadata
	 * @return the byte array
	 */
	public final byte[] getMetadata() {
		return m_metadata;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		if (m_metadata == null || m_metadata.length == 0) {
			p_buffer.putInt(0);
		} else {
			p_buffer.putInt(m_metadata.length);
			p_buffer.put(m_metadata);
		}
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		int length;

		length = p_buffer.getInt();
		if (length != 0) {
			m_metadata = new byte[length];
			p_buffer.get(m_metadata);
		}
	}

	@Override
	protected final int getPayloadLength() {
		int ret;

		ret = Integer.BYTES;
		if (m_metadata != null && m_metadata.length > 0) {
			ret += m_metadata.length;
		}

		return ret;
	}

}
