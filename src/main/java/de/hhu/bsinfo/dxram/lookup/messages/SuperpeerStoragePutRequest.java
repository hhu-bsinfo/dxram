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

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;

/**
 * Request to put data into the superpeer storage.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.2015
 */
public class SuperpeerStoragePutRequest extends Request {
    // DataStructures used when sending the put request.
    // These are also used by the response to directly write the
    // receiving data to the structures
    // Chunks are created and used when receiving a put request
    private AbstractChunk m_chunk;

    // used when receiving message
    private ChunkByteArray m_chunkRecv;

    private boolean m_isReplicate;

    private long m_chunkID; // Used for serialization, only

    /**
     * Creates an instance of SuperpeerStoragePutRequest.
     * This constructor is used when receiving this message.
     */
    public SuperpeerStoragePutRequest() {
        super();
    }

    /**
     * Creates an instance of SuperpeerStoragePutRequest
     *
     * @param p_destination
     *         the destination
     * @param p_chunk
     *         Data structure with the data to put.
     * @param p_replicate
     *         True if this message is a replication to other superpeer message, false if normal message
     */
    public SuperpeerStoragePutRequest(final short p_destination, final AbstractChunk p_chunk,
            final boolean p_replicate) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
                LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_REQUEST);

        m_chunk = p_chunk;
        m_isReplicate = p_replicate;
    }

    /**
     * Get the Chunks to put when this message is received.
     *
     * @return the Chunks to put
     */
    public final ChunkByteArray getChunk() {
        return m_chunkRecv;
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
            size += Long.BYTES + Integer.BYTES + m_chunk.sizeofObject() + Byte.BYTES;
        } else {
            size += Long.BYTES + Integer.BYTES + m_chunkRecv.sizeofObject() + Byte.BYTES;
        }

        return size;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        int size = m_chunk.sizeofObject();

        p_exporter.writeLong(m_chunk.getID());
        p_exporter.writeInt(size);
        p_exporter.exportObject(m_chunk);
        p_exporter.writeBoolean(m_isReplicate);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_chunkID = p_importer.readLong(m_chunkID);
        int size = p_importer.readInt(0);

        if (m_chunkRecv == null) {
            m_chunkRecv = new ChunkByteArray(m_chunkID, size);
        }

        p_importer.importObject(m_chunk);
        m_isReplicate = p_importer.readBoolean(m_isReplicate);
    }
}
