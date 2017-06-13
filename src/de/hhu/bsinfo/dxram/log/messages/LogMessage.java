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

package de.hhu.bsinfo.dxram.log.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.backup.RangeID;
import de.hhu.bsinfo.utils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.net.core.AbstractMessage;

/**
 * Message for logging a Chunk on a remote node
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.04.2016
 */
public class LogMessage extends AbstractMessage {

    // Attributes
    private short m_rangeID;
    private DataStructure[] m_dataStructures;
    private ByteBuffer m_buffer;

    private int m_copiedBytes;

    // Constructors

    /**
     * Creates an instance of LogMessage
     */
    public LogMessage() {
        super();

        m_rangeID = RangeID.INVALID_ID;
        m_dataStructures = null;
        m_buffer = null;
    }

    /**
     * Creates an instance of LogMessage
     *
     * @param p_destination
     *     the destination
     * @param p_dataStructures
     *     the data structures to store
     * @param p_rangeID
     *     the RangeID
     */
    public LogMessage(final short p_destination, final short p_rangeID, final DataStructure... p_dataStructures) {
        super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_LOG_MESSAGE, true);

        m_rangeID = p_rangeID;
        m_dataStructures = p_dataStructures;
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
        if (m_dataStructures != null) {
            int ret = Short.BYTES + Integer.BYTES;

            for (DataStructure dataStructure : m_dataStructures) {
                ret += Long.BYTES + Integer.BYTES + dataStructure.sizeofObject();
            }

            return ret;
        } else {
            return m_copiedBytes;
        }
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putShort(m_rangeID);

        p_buffer.putInt(m_dataStructures.length);
        final ByteBufferImExporter exporter = new ByteBufferImExporter(p_buffer);
        for (DataStructure dataStructure : m_dataStructures) {
            final int size = dataStructure.sizeofObject();

            p_buffer.putLong(dataStructure.getID());
            p_buffer.putInt(size);
            exporter.exportObject(dataStructure);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer, final int p_payloadSize, final boolean p_wasCopied) {
        if (p_wasCopied) {
            // De-serialize later
            m_buffer = p_buffer;
            m_copiedBytes = 0;
        } else {
            // Message buffer will be re-used -> copy data for later de-serialization
            m_buffer = ByteBuffer.allocate(p_payloadSize);
            m_buffer.put(p_buffer.array(), p_buffer.position(), p_payloadSize);
            p_buffer.position(p_buffer.position() + p_payloadSize);
            m_buffer.rewind();

            m_copiedBytes = p_payloadSize;
        }
    }

}
