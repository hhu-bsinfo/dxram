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

import de.hhu.bsinfo.utils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxram.data.ChunkAnon;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.net.core.AbstractMessage;

/**
 * (Async) Message for updating an anonymous Chunk
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 30.03.2017
 */
public class PutAnonMessage extends AbstractMessage {

    // chunks used when sending the put message
    private ChunkAnon[] m_chunks;

    // Variables used when receiving the message
    private long[] m_chunkIDs;
    private byte[][] m_data;

    /**
     * Creates an instance of PutAnonMessage.
     * This constructor is used when receiving this message.
     */
    public PutAnonMessage() {
        super();
    }

    /**
     * Creates an instance of PutAnonMessage
     *
     * @param p_destination
     *     the destination
     * @param p_unlockOperation
     *     if true a potential lock will be released
     * @param p_chunks
     *     Chunks with the data to put.
     */
    public PutAnonMessage(final short p_destination, final ChunkLockOperation p_unlockOperation, final ChunkAnon... p_chunks) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_ANON_MESSAGE);

        m_chunks = p_chunks;

        byte tmpCode = getStatusCode();
        switch (p_unlockOperation) {
            case NO_LOCK_OPERATION:
                break;
            case READ_LOCK:
                ChunkMessagesMetadataUtils.setReadLockFlag(tmpCode, true);
                break;
            case WRITE_LOCK:
                ChunkMessagesMetadataUtils.setWriteLockFlag(tmpCode, true);
                break;
            default:
                break;
        }

        setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(tmpCode, p_chunks.length));
    }

    /**
     * Get the chunk IDs of the data to put when this message is received.
     *
     * @return the IDs of the chunks to put
     */
    public long[] getChunkIDs() {
        return m_chunkIDs;
    }

    /**
     * Get the data of the chunks to put when this message is received
     *
     * @return Array of byte[] of chunk data to put
     */
    public byte[][] getChunkData() {
        return m_data;
    }

    /**
     * Get the unlock operation to execute after the put.
     *
     * @return Unlock operation.
     */
    public ChunkLockOperation getUnlockOperation() {
        if (ChunkMessagesMetadataUtils.isLockAcquireFlagSet(getStatusCode())) {
            if (ChunkMessagesMetadataUtils.isReadLockFlagSet(getStatusCode())) {
                return ChunkLockOperation.READ_LOCK;
            } else {
                return ChunkLockOperation.WRITE_LOCK;
            }
        } else {
            return ChunkLockOperation.NO_LOCK_OPERATION;
        }
    }

    @Override
    protected final int getPayloadLength() {
        int size = ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode());

        if (m_chunks != null) {
            size += m_chunks.length * Long.BYTES;

            for (ChunkAnon chunk : m_chunks) {
                size += chunk.sizeofObject();
            }
        } else {
            size += m_chunkIDs.length * Long.BYTES;

            for (int i = 0; i < m_data.length; i++) {
                size += Integer.BYTES + m_data[i].length;
            }
        }

        return size;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_chunks.length);

        ByteBufferImExporter exporter = new ByteBufferImExporter(p_buffer);
        for (ChunkAnon chunk : m_chunks) {
            p_buffer.putLong(chunk.getID());
            // the Chunk will write the size of its buffer as well
            exporter.exportObject(chunk);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int numChunks = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);

        m_chunkIDs = new long[numChunks];
        m_data = new byte[numChunks][];

        for (int i = 0; i < m_chunkIDs.length; i++) {
            m_chunkIDs[i] = p_buffer.getLong();
            m_data[i] = new byte[p_buffer.getInt()];

            p_buffer.get(m_data[i]);
        }
    }
}
