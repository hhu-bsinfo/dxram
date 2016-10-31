package de.hhu.bsinfo.dxram.logger.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * Set the log level of a remote node
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 19.04.2016
 */
public class SetLogLevelMessage extends AbstractMessage {

    private String m_logLevel;

    /**
     * Creates an instance of UnlockRequest as a receiver.
     */
    public SetLogLevelMessage() {
        super();
    }

    /**
     * Creates an instance of UnlockRequest as a sender
     *
     * @param p_destination
     *         the destination node ID.
     * @param p_logLevel
     *         Log level to set on remote node
     */
    public SetLogLevelMessage(final short p_destination, final String p_logLevel) {
        super(p_destination, DXRAMMessageTypes.LOGGER_MESSAGES_TYPE, LoggerMessages.SUBTYPE_SET_LOG_LEVEL_MESSAGE);

        m_logLevel = p_logLevel;
    }

    /**
     * Get the log level to set.
     *
     * @return LogLevel
     */
    public String getLogLevel() {
        return m_logLevel;
    }

    // Methods
    @Override protected final void writePayload(final ByteBuffer p_buffer) {
        byte[] b = m_logLevel.getBytes();

        p_buffer.putInt(b.length);
        p_buffer.put(b);
    }

    @Override protected final void readPayload(final ByteBuffer p_buffer) {

        byte[] b = new byte[p_buffer.getInt()];
        p_buffer.get(b);
        m_logLevel = new String(b);
    }

    @Override protected final int getPayloadLength() {
        return Integer.BYTES + m_logLevel.getBytes().length;
    }

}
