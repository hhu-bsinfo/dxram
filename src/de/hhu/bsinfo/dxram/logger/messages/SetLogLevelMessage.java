
package de.hhu.bsinfo.dxram.logger.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.utils.logger.LogLevel;

/**
 * Set the log level of a remote node
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 19.04.16
 */
public class SetLogLevelMessage extends AbstractMessage {

	private LogLevel m_logLevel;

	/**
	 * Creates an instance of UnlockRequest as a receiver.
	 */
	public SetLogLevelMessage() {
		super();
	}

	/**
	 * Creates an instance of UnlockRequest as a sender
	 * @param p_destination
	 *            the destination node ID.
	 * @param p_logLevel
	 *            Log level to set on remote node
	 */
	public SetLogLevelMessage(final short p_destination, final LogLevel p_logLevel) {
		super(p_destination, DXRAMMessageTypes.LOGGER_MESSAGES_TYPE, LoggerMessages.SUBTYPE_SET_LOG_LEVEL_MESSAGE);

		m_logLevel = p_logLevel;
	}

	/**
	 * Get the log level to set.
	 * @return LogLevel
	 */
	public LogLevel getLogLevel() {
		return m_logLevel;
	}

	// Methods
	@Override
	protected final void writePayload(final ByteBuffer p_buffer) {
		p_buffer.putInt(m_logLevel.ordinal());
	}

	@Override
	protected final void readPayload(final ByteBuffer p_buffer) {
		m_logLevel = LogLevel.values()[p_buffer.getInt()];
	}

	@Override
	protected final int getPayloadLength() {
		return Integer.BYTES;
	}

}
