/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.chunk.messages;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkLockOperation;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Request for updating a Chunk on a remote node
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public class PutRequest extends Request {
    // used on both, sending and receiving
    private ChunkLockOperation m_lockOperation = ChunkLockOperation.NONE;
    private int m_lockOperationTimeoutMs = -1;

    // used when sending the request
    private AbstractChunk m_chunk;

    // used when receiving the request
    private long m_chunkID;
    private int m_dataLength;
    private byte[] m_data;

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
     *         the destination node id.
     * @param p_lockOperation
     *         Lock operation to execute with put operation
     * @param p_lockOperationTimeoutMs
     *         Timeout for lock operation. -1 for infinite, 0 for one shot, > 0 timeout in ms
     * @param p_chunk
     *         Chunk with the ID of the chunk data to put.
     */
    public PutRequest(final short p_destination, final ChunkLockOperation p_lockOperation,
            final int p_lockOperationTimeoutMs, final AbstractChunk p_chunk) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_REQUEST);

        m_lockOperation = p_lockOperation;
        m_lockOperationTimeoutMs = p_lockOperationTimeoutMs;
        m_chunk = p_chunk;
    }

    /**
     * Get the lock operation to execute with the put operation
     *
     * @return Lock operation to execute with put
     */
    public ChunkLockOperation getLockOperation() {
        return m_lockOperation;
    }

    /**
     * Get the timeout value for the lock operation
     *
     * @return Timeout value for lock operation
     */
    public int getLockOperationTimeoutMs() {
        return m_lockOperationTimeoutMs;
    }

    /**
     * Get the chunk to put (available on sender side, only)
     *
     * @return Chunk to put
     */
    public AbstractChunk getChunk() {
        return m_chunk;
    }

    /**
     * Get the chunk ID of the data to put when this request is received.
     *
     * @return the ID of the chunk to put
     */
    public long getChunkID() {
        return m_chunkID;
    }

    /**
     * Get the data of the chunk to put when this request is received
     *
     * @return Binary data of chunk data to put
     */
    public byte[] getChunkData() {
        return m_data;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        size += Byte.BYTES;

        // omit timeout field if lock operation is none
        if (m_lockOperation != ChunkLockOperation.NONE) {
            size += Integer.BYTES;
        }

        // chunk ID
        size += Long.BYTES;

        if (m_chunk != null) {
            int tmp = m_chunk.sizeofObject();

            // length field for for data
            size += ObjectSizeUtil.sizeofCompactedNumber(tmp);
            // data size
            size += tmp;
        } else {
            size += ObjectSizeUtil.sizeofByteArray(m_data);
        }

        return size;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeByte((byte) m_lockOperation.ordinal());

        if (m_lockOperation != ChunkLockOperation.NONE) {
            p_exporter.writeInt(m_lockOperationTimeoutMs);
        }

        int size = m_chunk.sizeofObject();

        p_exporter.writeLong(m_chunk.getID());
        p_exporter.writeCompactNumber(size);
        p_exporter.exportObject(m_chunk);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_lockOperation = ChunkLockOperation.values()[p_importer.readByte((byte) m_lockOperation.ordinal())];

        if (m_lockOperation != ChunkLockOperation.NONE) {
            m_lockOperationTimeoutMs = p_importer.readInt(m_lockOperationTimeoutMs);
        }

        m_chunkID = p_importer.readLong(m_chunkID);
        m_data = p_importer.readByteArray(m_data);
    }
}
