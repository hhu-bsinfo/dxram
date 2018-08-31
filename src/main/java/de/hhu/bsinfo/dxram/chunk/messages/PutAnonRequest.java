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
import de.hhu.bsinfo.dxram.chunk.data.ChunkAnon;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Request for updating a Chunk using an anonymous Chunk on a remote node
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 30.03.2017
 */
public class PutAnonRequest extends Request {
    // used on both, sending and receiving
    private ChunkLockOperation m_lockOperation = ChunkLockOperation.NONE;
    private int m_lockOperationTimeoutMs = -1;

    // used when sending the request
    private ChunkAnon[] m_chunks;

    // used when receiving the request
    private long[] m_chunkIDs;
    private byte[][] m_data;

    // necessary to guarantee correct values on split messages/buffers
    private int m_compactNumberTmpState;

    /**
     * Creates an instance of PutAnonRequest.
     * This constructor is used when receiving this message.
     */
    public PutAnonRequest() {
        super();
    }

    /**
     * Creates an instance of PutAnonRequest
     *
     * @param p_destination
     *         the destination node id.
     * @param p_lockOperation
     *         Lock operation to execute with put operation
     * @param p_lockOperationTimeoutMs
     *         Timeout for lock operation. -1 for infinite, 0 for one shot, > 0 timeout in ms
     * @param p_chunks
     *         Chunks with the ID of the chunk data to put.
     */
    public PutAnonRequest(final short p_destination, final ChunkLockOperation p_lockOperation,
            final int p_lockOperationTimeoutMs, final ChunkAnon... p_chunks) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_ANON_REQUEST);

        m_lockOperation = p_lockOperation;
        m_lockOperationTimeoutMs = p_lockOperationTimeoutMs;
        m_chunks = p_chunks;
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

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        size += Byte.BYTES;

        // omit timeout field if lock operation is none
        if (m_lockOperation != ChunkLockOperation.NONE) {
            size += Integer.BYTES;
        }

        if (m_chunks != null) {
            // sending request with chunk objects
            size += ObjectSizeUtil.sizeofCompactedNumber(m_chunks.length);
            // chunk IDs
            size += m_chunks.length * Long.BYTES;

            for (AbstractChunk chunk : m_chunks) {
                size += chunk.sizeofObject();
            }
        } else {
            // receiving request. chunk data as byte array only, no type information
            // chunk IDs
            size += ObjectSizeUtil.sizeofCompactedNumber(m_chunkIDs.length);
            size += m_chunkIDs.length * Long.BYTES;

            for (byte[] data : m_data) {
                size += ObjectSizeUtil.sizeofByteArray(data);
            }
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

        p_exporter.writeCompactNumber(m_chunks.length);

        for (AbstractChunk chunk : m_chunks) {
            p_exporter.writeLong(chunk.getID());
            p_exporter.exportObject(chunk);
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_lockOperation = ChunkLockOperation.values()[p_importer.readByte((byte) m_lockOperation.ordinal())];

        if (m_lockOperation != ChunkLockOperation.NONE) {
            m_lockOperationTimeoutMs = p_importer.readInt(m_lockOperationTimeoutMs);
        }

        m_compactNumberTmpState = p_importer.readCompactNumber(m_compactNumberTmpState);

        if (m_chunkIDs == null) {
            // Do not overwrite existing arrays
            m_chunkIDs = new long[m_compactNumberTmpState];
            m_data = new byte[m_compactNumberTmpState][];
        }

        for (int i = 0; i < m_chunkIDs.length; i++) {
            m_chunkIDs[i] = p_importer.readLong(m_chunkIDs[i]);
            m_data[i] = p_importer.readByteArray(m_data[i]);
        }
    }
}
