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

import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.utils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.core.AbstractRequest;

/**
 * Request for updating a Chunk on a remote node
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public class PutRequest extends AbstractRequest {

    // DataStructures used when sending the put request.
    private DataStructure[] m_dataStructures;

    // Variables used when receiving the request
    private long[] m_chunkIDs;
    private byte[][] m_data;

    /**
     * Creates an instance of PutRequest.
     * This constructor is used when receiving this message.
     */
    public PutRequest() {
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
    public PutRequest(final short p_destination, final ChunkLockOperation p_unlockOperation, final DataStructure... p_dataStructures) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_REQUEST);

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
                assert false;
                break;
        }

        setStatusCode(ChunkMessagesMetadataUtils.setNumberOfItemsToSend(tmpCode, p_dataStructures.length));
    }

    /**
     * Get the chunk IDs of the data to put when this request is received.
     *
     * @return the IDs of the chunks to put
     */
    public long[] getChunkIDs() {
        return m_chunkIDs;
    }

    /**
     * Get the data of the chunks to put when this request is received
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

        if (m_dataStructures != null) {
            size += m_dataStructures.length * Long.BYTES;
            size += m_dataStructures.length * Integer.BYTES;

            for (DataStructure dataStructure : m_dataStructures) {
                size += dataStructure.sizeofObject();
            }
        } else {
            size += m_chunkIDs.length * Long.BYTES;
            size += m_chunkIDs.length * Integer.BYTES;

            for (byte[] byteArray : m_data) {
                size += byteArray.length;
            }
        }

        return size;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        ChunkMessagesMetadataUtils.setNumberOfItemsInMessageBuffer(getStatusCode(), p_buffer, m_dataStructures.length);

        ByteBufferImExporter exporter = new ByteBufferImExporter(p_buffer);
        for (DataStructure dataStructure : m_dataStructures) {
            int size = dataStructure.sizeofObject();

            p_buffer.putLong(dataStructure.getID());
            p_buffer.putInt(size);
            exporter.exportObject(dataStructure);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int numChunks = ChunkMessagesMetadataUtils.getNumberOfItemsFromMessageBuffer(getStatusCode(), p_buffer);

        m_chunkIDs = new long[numChunks];
        m_data = new byte[numChunks][];

        for (int i = 0; i < numChunks; i++) {
            m_chunkIDs[i] = p_buffer.getLong();
            m_data[i] = new byte[p_buffer.getInt()];

            p_buffer.get(m_data[i]);
        }
    }
}
