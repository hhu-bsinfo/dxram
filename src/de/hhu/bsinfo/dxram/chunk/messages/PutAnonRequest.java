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

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.data.ChunkAnon;
import de.hhu.bsinfo.dxram.data.ChunkLockOperation;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Request for updating a Chunk using an anonymous Chunk on a remote node
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 30.03.2017
 */
public class PutAnonRequest extends Request {
    // chunk used when sending the put request.
    private ChunkAnon[] m_chunks;

    private byte m_lockCode;

    // Variables used when receiving the request
    private long[] m_chunkIDs;
    private byte[][] m_data;

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
     *         the destination
     * @param p_unlockOperation
     *         if true a potential lock will be released
     * @param p_chunks
     *         Chunk buffers with the data to put.
     */
    public PutAnonRequest(final short p_destination, final ChunkLockOperation p_unlockOperation, final ChunkAnon... p_chunks) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_PUT_ANON_REQUEST);

        m_chunks = p_chunks;
        switch (p_unlockOperation) {
            case NO_LOCK_OPERATION:
                m_lockCode = 0;
                break;
            case READ_LOCK:
                m_lockCode = 1;
                break;
            case WRITE_LOCK:
                m_lockCode = 2;
                break;
            default:
                break;
        }
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
        if (m_lockCode == 0) {
            return ChunkLockOperation.NO_LOCK_OPERATION;
        } else if (m_lockCode == 1) {
            return ChunkLockOperation.READ_LOCK;
        } else {
            return ChunkLockOperation.WRITE_LOCK;
        }
    }

    @Override
    protected final int getPayloadLength() {
        int size = Byte.BYTES;

        if (m_chunks != null) {
            size += ObjectSizeUtil.sizeofCompactedNumber(m_chunks.length);
            size += m_chunks.length * Long.BYTES;

            for (ChunkAnon chunk : m_chunks) {
                size += chunk.sizeofObject();
            }
        } else {
            size += ObjectSizeUtil.sizeofCompactedNumber(m_chunkIDs.length);
            size += m_chunkIDs.length * Long.BYTES;

            for (int i = 0; i < m_data.length; i++) {
                size += ObjectSizeUtil.sizeofByteArray(m_data[i]);
            }
        }

        return size;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeByte(m_lockCode);

        p_exporter.writeCompactNumber(m_chunks.length);
        for (ChunkAnon chunk : m_chunks) {
            p_exporter.writeLong(chunk.getID());
            // the Chunk will write the size of its buffer as well
            p_exporter.exportObject(chunk);
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_lockCode = p_importer.readByte(m_lockCode);

        int length = p_importer.readCompactNumber(0);
        if (m_chunkIDs == null) {
            // Do not overwrite existing arrays
            m_chunkIDs = new long[length];
            m_data = new byte[length][];
        }

        for (int i = 0; i < m_chunkIDs.length; i++) {
            m_chunkIDs[i] = p_importer.readLong(m_chunkIDs[i]);
            m_data[i] = p_importer.readByteArray(m_data[i]);
        }
    }
}
