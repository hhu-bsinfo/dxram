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

package de.hhu.bsinfo.dxram.chunk.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.utils.ArrayListLong;
import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * Request for removing a Chunk on a remote node
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public class RemoveMessage extends AbstractMessage {

    private ArrayListLong m_chunkIDsOut;
    private long[] m_chunkIDs;

    /**
     * Creates an instance of RemoveMessage.
     * This constructor is used when receiving this message.
     */
    public RemoveMessage() {
        super();
    }

    /**
     * Creates an instance of RemoveMessage.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *     the destination
     * @param p_chunkIds
     *     the chunk IDs to remove
     */
    public RemoveMessage(final short p_destination, final ArrayListLong p_chunkIds) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_REMOVE_REQUEST);

        m_chunkIDsOut = p_chunkIds;

        setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(getStatusCode(), m_chunkIDsOut.getSize()));
    }

    /**
     * Get the ID for the Chunk to remove
     *
     * @return the ID for the Chunk to remove
     */
    public final long[] getChunkIDs() {
        return m_chunkIDs;
    }

    @Override
    protected final int getPayloadLength() {
        int size = ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode());

        if (m_chunkIDsOut != null) {
            size += Long.BYTES * m_chunkIDsOut.getSize();
        } else {
            size += Long.BYTES * m_chunkIDs.length;
        }

        return size;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {

        if (m_chunkIDsOut != null) {
            ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_chunkIDsOut.getSize());

            for (int i = 0; i < m_chunkIDsOut.getSize(); i++) {
                p_buffer.putLong(m_chunkIDsOut.get(i));
            }
        } else {
            ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_chunkIDs.length);

            for (long chunkId : m_chunkIDs) {
                p_buffer.putLong(chunkId);
            }
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int numChunks = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);

        m_chunkIDs = new long[numChunks];

        for (int i = 0; i < numChunks; i++) {
            m_chunkIDs[i] = p_buffer.getLong();
        }
    }

}
