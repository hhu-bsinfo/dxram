/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.util.ArrayListLong;
import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * Message for removing a Chunk on a remote node
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 20.04.2016
 */
public class RemoveMessage extends AbstractMessage {

    // Attributes
    private ArrayListLong m_chunkIDs;
    private byte m_rangeID;
    private ByteBuffer m_buffer;

    // Constructors

    /**
     * Creates an instance of RemoveMessage
     */
    public RemoveMessage() {
        super();

        m_chunkIDs = null;
        m_rangeID = -1;
        m_buffer = null;
    }

    /**
     * Creates an instance of RemoveMessage
     *
     * @param p_destination
     *     the destination
     * @param p_chunkIDs
     *     the ChunkIDs of the Chunks to remove
     */
    public RemoveMessage(final short p_destination, final ArrayListLong p_chunkIDs) {
        super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_REMOVE_MESSAGE, true);

        m_chunkIDs = p_chunkIDs;
        m_rangeID = -1;
    }

    /**
     * Creates an instance of RemoveMessage
     *
     * @param p_destination
     *     the destination
     * @param p_chunkIDs
     *     the ChunkIDs of the Chunks to remove
     */
    public RemoveMessage(final short p_destination, final long[] p_chunkIDs) {
        super(p_destination, DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_REMOVE_MESSAGE, true);

        m_chunkIDs = ArrayListLong.wrap(p_chunkIDs);
        m_rangeID = -1;
    }

    /**
     * Creates an instance of RemoveMessage
     *
     * @param p_destination
     *     the destination
     * @param p_chunkIDs
     *     the ChunkIDs of the Chunks to remove
     * @param p_rangeID
     *     the RangeID
     */
    public RemoveMessage(final short p_destination, final ArrayListLong p_chunkIDs, final byte p_rangeID) {
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
            return Byte.BYTES + Integer.BYTES + Long.BYTES * m_chunkIDs.getSize();
        } else {
            return 0;
        }
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.put(m_rangeID);
        p_buffer.putInt(m_chunkIDs.getSize());
        p_buffer.asLongBuffer().put(m_chunkIDs.getArray(), 0, m_chunkIDs.getSize());
        p_buffer.position(p_buffer.position() + m_chunkIDs.getSize() * Long.BYTES);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_buffer = p_buffer;
    }

}
