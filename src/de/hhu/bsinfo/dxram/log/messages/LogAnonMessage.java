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

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.RangeID;
import de.hhu.bsinfo.dxram.data.ChunkAnon;
import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;

/**
 * Message for logging an anonymous chunk on a remote node
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.04.2016
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.03.2017
 */
public class LogAnonMessage extends AbstractMessage {

    // Attributes
    private short m_rangeID;
    private ChunkAnon[] m_chunks;
    private ByteBuffer m_buffer;

    private int m_copiedBytes;

    // Constructors

    /**
     * Creates an instance of LogMessage
     */
    public LogAnonMessage() {
        super();

        m_rangeID = RangeID.INVALID_ID;
        m_chunks = null;
        m_buffer = null;
    }

    /**
     * Creates an instance of LogMessage
     *
     * @param p_destination
     *         the destination
     * @param p_chunks
     *         the chunks to store
     * @param p_rangeID
     *         the RangeID
     */
    public LogAnonMessage(final short p_destination, final short p_rangeID, final ChunkAnon... p_chunks) {
        super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_LOG_ANON_MESSAGE, true);

        m_rangeID = p_rangeID;
        m_chunks = p_chunks;
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
        if (m_chunks != null) {
            int ret = Short.BYTES + Integer.BYTES;

            for (ChunkAnon chunk : m_chunks) {
                ret += Long.BYTES + chunk.sizeofObject();
            }

            return ret;
        } else {
            return m_copiedBytes;
        }
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_rangeID);

        p_exporter.writeInt(m_chunks.length);
        for (ChunkAnon chunk : m_chunks) {
            p_exporter.writeLong(chunk.getID());
            p_exporter.exportObject(chunk);
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer, final ByteBuffer p_buffer, final int p_payloadSize, final boolean p_wasCopied) {
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
