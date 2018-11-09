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
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Request for locking/unlocking a chunk on a remote node.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.11.2018
 */
public class LockRequest extends Request {
    private boolean m_lock;
    private boolean m_writeLock;
    private int m_lockOperationTimeoutMs = -1;
    private AbstractChunk[] m_chunks;

    // this is only used when receiving the request
    private long[] m_chunkIDs;

    // state for deserialization
    private int m_tmpSize;

    /**
     * Creates an instance of LockRequest.
     * This constructor is used when receiving this message.
     */
    public LockRequest() {
        super();
    }

    /**
     * Creates an instance of LockRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination node id.
     * @param p_lock
     *         True to lock the chunks, false to unlock
     * @param p_writeLock
     *         True to use a write lock, false for a read lock
     * @param p_lockOperationTimeoutMs
     *         If a lock operation is set, set to -1 for infinite retries (busy polling) until the lock operation
     *         succeeds. 0 for a one shot try and &gt; 0 for a timeout value in ms
     * @param p_chunks
     *         Chunks with the ID to lock/unlock
     */
    public LockRequest(final short p_destination, final boolean p_lock, final boolean p_writeLock,
            final int p_lockOperationTimeoutMs, final AbstractChunk... p_chunks) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_LOCK_REQUEST);

        m_lock = p_lock;
        m_writeLock = p_writeLock;
        m_lockOperationTimeoutMs = p_lockOperationTimeoutMs;
        m_chunks = p_chunks;
    }

    /**
     * Operation to execute
     *
     * @return True to lock, false to unlock
     */
    public boolean lock() {
        return m_lock;
    }

    /**
     * Type of lock
     *
     * @return True if write lock, false for read lock
     */
    public boolean isWriteLock() {
        return m_writeLock;
    }

    /**
     * Get the lock operation timeout in ms
     *
     * @return Timeout in ms
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

    @Override
    protected final int getPayloadLength() {
        if (m_chunks != null) {
            return Byte.BYTES * 2 + Integer.BYTES + ObjectSizeUtil.sizeofCompactedNumber(m_chunks.length) +
                    Long.BYTES * m_chunks.length;
        } else {
            return Byte.BYTES * 2 + Integer.BYTES + ObjectSizeUtil.sizeofCompactedNumber(m_chunkIDs.length) +
                    Long.BYTES * m_chunkIDs.length;
        }
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeBoolean(m_lock);
        p_exporter.writeBoolean(m_writeLock);
        p_exporter.writeInt(m_lockOperationTimeoutMs);

        p_exporter.writeCompactNumber(m_chunks.length);

        for (AbstractChunk chunk : m_chunks) {
            p_exporter.writeLong(chunk.getID());
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_lock = p_importer.readBoolean(m_lock);
        m_writeLock = p_importer.readBoolean(m_writeLock);
        m_lockOperationTimeoutMs = p_importer.readInt(m_lockOperationTimeoutMs);

        m_chunkIDs = p_importer.readLongArray(m_chunkIDs);
    }
}
