/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

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
     *     the destination node ID.
     * @param p_logLevel
     *     Log level to set on remote node
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

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + m_logLevel.getBytes().length;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        byte[] array = m_logLevel.getBytes();

        p_buffer.putInt(array.length);
        p_buffer.put(array);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {

        byte[] array = new byte[p_buffer.getInt()];
        p_buffer.get(array);
        m_logLevel = new String(array);
    }

}
