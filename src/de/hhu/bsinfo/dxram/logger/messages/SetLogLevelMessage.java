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

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Set the log level of a remote node
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 19.04.2016
 */
public class SetLogLevelMessage extends Message {

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

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofString(m_logLevel);
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeString(m_logLevel);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_logLevel = p_importer.readString(m_logLevel);
    }

}
