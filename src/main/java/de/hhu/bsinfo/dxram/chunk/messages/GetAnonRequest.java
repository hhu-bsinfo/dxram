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
 * Request for getting an anonymous chunk from a remote node. The size of a chunk is _NOT_ known prior fetching the data
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 30.03.2017
 */
public class GetAnonRequest extends Request {
    private ChunkLockOperation m_lockOperation = ChunkLockOperation.NONE;
    private int m_lockOperationTimeoutMs = -1;
    // the chunk is stored for the sender of the request
    // to write the incoming data of the response to it
    // the requesting IDs are taken from the chunk
    private ChunkAnon[] m_chunks;
    // this is only used when receiving the request
    private long[] m_chunkIDs;

    /**
     * Creates an instance of GetAnonRequest.
     * This constructor is used when receiving this message.
     */
    public GetAnonRequest() {
        super();
    }

    /**
     * Creates an instance of GetAnonRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination node id.
     * @param p_lockOperation
     *         Lock operation to execute with get operation
     * @param p_lockOperationTimeoutMs
     *         Timeout for lock operation. -1 for inifinte, 0 for one shot, > 0 timeout in ms
     * @param p_chunks
     *         Chunks with the ID of the chunk data to get.
     */
    public GetAnonRequest(final short p_destination, final ChunkLockOperation p_lockOperation,
            final int p_lockOperationTimeoutMs, final ChunkAnon... p_chunks) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_ANON_REQUEST);

        m_lockOperation = p_lockOperation;
        m_lockOperationTimeoutMs = p_lockOperationTimeoutMs;
        m_chunks = p_chunks;
    }

    /**
     * Get the lock operation to execute with the get operation
     *
     * @return Lock operation to execute with get
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
     * Get the chunk IDs of this request (when receiving it).
     *
     * @return Chunk ID.
     */
    public long[] getChunkIDs() {
        return m_chunkIDs;
    }

    /**
     * Get the chunks stored with this request.
     * This is used to write the received data to the provided object to avoid
     * using multiple buffers.
     *
     * @return Chunks to store data to when the response arrived.
     */
    public ChunkAnon[] getChunks() {
        return m_chunks;
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
            size += ObjectSizeUtil.sizeofCompactedNumber(m_chunks.length) + Long.BYTES * m_chunks.length;
        } else {
            size += ObjectSizeUtil.sizeofCompactedNumber(m_chunkIDs.length) + Long.BYTES * m_chunkIDs.length;
        }

        return size;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeByte((byte) m_lockOperation.ordinal());

        if (m_lockOperation != ChunkLockOperation.NONE) {
            p_exporter.writeInt(m_lockOperationTimeoutMs);
        }

        p_exporter.writeCompactNumber(m_chunks.length);

        for (AbstractChunk chunk : m_chunks) {
            p_exporter.writeLong(chunk.getID());
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_lockOperation = ChunkLockOperation.values()[p_importer.readByte((byte) m_lockOperation.ordinal())];

        if (m_lockOperation != ChunkLockOperation.NONE) {
            m_lockOperationTimeoutMs = p_importer.readInt(m_lockOperationTimeoutMs);
        }

        m_chunkIDs = p_importer.readLongArray(m_chunkIDs);
    }
}
