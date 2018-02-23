/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.log.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.RangeID;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;

/**
 * Message for logging an anonymous chunk on a remote node
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.04.2016
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.03.2017
 */
public class LogBufferMessage extends Message {

    // Attributes
    private short m_rangeID;
    private ByteBuffer m_buffer;
    private int m_bytesRead;

    // Constructors

    /**
     * Creates an instance of LogMessage
     */
    public LogBufferMessage() {
        super();

        m_rangeID = RangeID.INVALID_ID;
        m_buffer = null;
    }

    /**
     * Creates an instance of LogMessage
     *
     * @param p_destination
     *         the destination
     * @param p_rangeID
     *         the RangeID
     * @param p_buffer
     *         the chunks to store with ChunkID and payload size prepended
     */
    public LogBufferMessage(final short p_destination, final short p_rangeID, final ByteBuffer p_buffer) {
        super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_LOG_BUFFER_MESSAGE, true);

        m_rangeID = p_rangeID;
        m_buffer = p_buffer;
        m_bytesRead = m_buffer.limit() + Short.BYTES;
    }

    // Getters

    /**
     * Get the message buffer
     *
     * @return the message buffer
     */
    public final ByteBuffer getMessageBuffer() {
        return m_buffer;
    }

    @Override
    protected final int getPayloadLength() {
        return m_bytesRead;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_rangeID);
        p_exporter.writeBytes(m_buffer.array());
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer, final int p_payloadSize) {
        // Just copy all bytes, will be serialized into primary write buffer later
        byte[] bytes = new byte[p_payloadSize];
        p_importer.readBytes(bytes);
        m_buffer = ByteBuffer.wrap(bytes);
        m_bytesRead = m_buffer.limit();
    }

}
