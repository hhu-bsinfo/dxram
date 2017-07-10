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
import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.utils.ArrayListLong;

/**
 * Message for removing a Chunk on a remote node
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.04.2016
 */
public class RemoveMessage extends AbstractMessage {

    // Attributes
    private ArrayListLong m_chunkIDs;
    private short m_rangeID;
    private ByteBuffer m_buffer;

    private int m_copiedBytes;

    // Constructors

    /**
     * Creates an instance of RemoveMessage
     */
    public RemoveMessage() {
        super();

        m_chunkIDs = null;
        m_rangeID = RangeID.INVALID_ID;
        m_buffer = null;
    }

    /**
     * Creates an instance of RemoveMessage
     *
     * @param p_destination
     *         the destination
     * @param p_chunkIDs
     *         the ChunkIDs of the Chunks to remove
     */
    public RemoveMessage(final short p_destination, final ArrayListLong p_chunkIDs) {
        super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_REMOVE_MESSAGE, true);

        m_chunkIDs = p_chunkIDs;
        m_rangeID = RangeID.INVALID_ID;
    }

    /**
     * Creates an instance of RemoveMessage
     *
     * @param p_destination
     *         the destination
     * @param p_chunkIDs
     *         the ChunkIDs of the Chunks to remove
     */
    public RemoveMessage(final short p_destination, final long[] p_chunkIDs) {
        super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_REMOVE_MESSAGE, true);

        m_chunkIDs = ArrayListLong.wrap(p_chunkIDs);
        m_rangeID = RangeID.INVALID_ID;
    }

    /**
     * Creates an instance of RemoveMessage
     *
     * @param p_destination
     *         the destination
     * @param p_chunkIDs
     *         the ChunkIDs of the Chunks to remove
     * @param p_rangeID
     *         the RangeID
     */
    public RemoveMessage(final short p_destination, final ArrayListLong p_chunkIDs, final short p_rangeID) {
        super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_REMOVE_MESSAGE, true);

        m_chunkIDs = p_chunkIDs;
        m_rangeID = p_rangeID;
    }

    // Getters

    /**
     * Get the RangeID
     *
     * @return the RangeID
     */
    public final ByteBuffer getMessageBuffer() {
        return m_buffer;
    }

    @Override
    protected final int getPayloadLength() {
        if (m_chunkIDs != null) {
            return Short.BYTES + Integer.BYTES + Long.BYTES * m_chunkIDs.getSize();
        } else {
            return m_copiedBytes;
        }
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeShort(m_rangeID);
        p_exporter.exportObject(m_chunkIDs);
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
