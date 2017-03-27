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

package de.hhu.bsinfo.dxram.chunk.messages;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * (Async) Message for updating a Chunk on a remote node
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public class PutMessage extends AbstractMessage {

    // DataStructures used when sending the put request.
    // These are also used by the response to directly write the
    // receiving data to the structures
    // Chunks are created and used when receiving a put request
    private DataStructure[] m_dataStructures;

    /**
     * Creates an instance of PutRequest.
     * This constructor is used when receiving this message.
     */
    public PutMessage() {
        super();
    }

    /**
     * Creates an instance of PutRequest
     *
     * @param p_destination
     *     the destination
     * @param p_unlockOperation
     *     if true a potential lock will be released
     * @param p_dataStructures
     *     Data structure with the data to put.
     */
    public PutMessage(final short p_destination, final ChunkLockOperation p_unlockOperation, final DataStructure... p_dataStructures) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_MESSAGE);

        m_dataStructures = p_dataStructures;

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

        setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(tmpCode, p_dataStructures.length));
    }

    /**
     * Get the DataStructures to put when this message is received.
     *
     * @return the Chunk to put
     */
    public final DataStructure[] getDataStructures() {
        return m_dataStructures;
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

        size += m_dataStructures.length * Long.BYTES;
        size += m_dataStructures.length * Integer.BYTES;

        for (DataStructure dataStructure : m_dataStructures) {
            size += dataStructure.sizeofObject();
        }

        return size;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_dataStructures.length);

        MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
        for (DataStructure dataStructure : m_dataStructures) {
            int size = dataStructure.sizeofObject();

            p_buffer.putLong(dataStructure.getID());
            p_buffer.putInt(size);
            p_buffer.order(ByteOrder.nativeOrder());
            exporter.exportObject(dataStructure);
            p_buffer.order(ByteOrder.BIG_ENDIAN);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);
        int numChunks = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);

        m_dataStructures = new Chunk[numChunks];

        for (int i = 0; i < m_dataStructures.length; i++) {
            long id = p_buffer.getLong();
            int size = p_buffer.getInt();

            m_dataStructures[i] = new Chunk(id, size);
            p_buffer.order(ByteOrder.nativeOrder());
            importer.importObject(m_dataStructures[i]);
            p_buffer.order(ByteOrder.BIG_ENDIAN);
        }
    }

}
