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

package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.chunk.data.ChunkAnon;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Request to put data into the superpeer storage.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 04.04.2017
 */
public class SuperpeerStoragePutAnonRequest extends Request {
    // chunk used when sending the put request.
    private ChunkAnon m_chunk;

    // used when receiving message
    private ChunkByteArray m_data;

    private boolean m_isReplicate;

    private long m_chunkID; // Used for serialization, only

    /**
     * Creates an instance of SuperpeerStoragePutAnonRequest.
     * This constructor is used when receiving this message.
     */
    public SuperpeerStoragePutAnonRequest() {
        super();
    }

    /**
     * Creates an instance of SuperpeerStoragePutAnonRequest
     *
     * @param p_destination
     *         the destination
     * @param p_chunk
     *         Chunk with the data to put.
     * @param p_replicate
     *         True if this message is a replication to other superpeer message, false if normal message
     */
    public SuperpeerStoragePutAnonRequest(final short p_destination, final ChunkAnon p_chunk,
            final boolean p_replicate) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_ANON_REQUEST);

        m_chunk = p_chunk;
        m_isReplicate = p_replicate;
    }

    /**
     * Get the Chunks to put when this message is received.
     *
     * @return the Chunks to put
     */
    public final ChunkByteArray getChunk() {
        return m_data;
    }

    /**
     * Check if this request is a replicate message.
     *
     * @return True if replicate message.
     */
    public boolean isReplicate() {
        return m_isReplicate;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        if (m_chunk != null) {
            size += Long.BYTES + m_chunk.sizeofObject() + Byte.BYTES;
        } else {
            // that's serialized without any length field information
            int byteArrayChunkSize = m_data.sizeofObject();
            size += Long.BYTES + ObjectSizeUtil.sizeofCompactedNumber(byteArrayChunkSize) + m_data.sizeofObject() +
                    Byte.BYTES;
        }

        return size;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeLong(m_chunk.getID());
        p_exporter.exportObject(m_chunk);
        p_exporter.writeBoolean(m_isReplicate);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_chunkID = p_importer.readLong(m_chunkID);
        int size = p_importer.readCompactNumber(0);
        if (m_data == null) {
            m_data = new ChunkByteArray(m_chunkID, size);
        }
        p_importer.importObject(m_data);
        m_isReplicate = p_importer.readBoolean(m_isReplicate);
    }
}
