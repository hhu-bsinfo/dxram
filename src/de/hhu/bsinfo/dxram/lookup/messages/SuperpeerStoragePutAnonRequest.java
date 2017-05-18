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

package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.utils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.dxram.data.ChunkAnon;
import de.hhu.bsinfo.dxram.data.ChunkMessagesMetadataUtils;
import de.hhu.bsinfo.dxram.data.DSByteArray;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request to put data into the superpeer storage.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 04.04.2017
 */
public class SuperpeerStoragePutAnonRequest extends AbstractRequest {
    // chunk used when sending the put request.
    private ChunkAnon m_chunk;

    // used when receiving message
    private DSByteArray m_data;

    private boolean m_isReplicate;

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
     *     the destination
     * @param p_chunk
     *     Chunk with the data to put.
     * @param p_replicate
     *     True if this message is a replication to other superpeer message, false if normal message
     */
    public SuperpeerStoragePutAnonRequest(final short p_destination, final ChunkAnon p_chunk, final boolean p_replicate) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_ANON_REQUEST);

        m_chunk = p_chunk;
        m_isReplicate = p_replicate;
    }

    /**
     * Get the Chunks to put when this message is received.
     *
     * @return the Chunks to put
     */
    public final DSByteArray getChunk() {
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
        int size = ChunkMessagesMetadataUtils.getSizeOfAdditionalLengthField(getStatusCode());

        if (m_chunk != null) {
            size += Long.BYTES + m_chunk.sizeofObject() + Byte.BYTES;
        } else {
            size += Long.BYTES + Integer.BYTES + m_data.sizeofObject() + Byte.BYTES;
        }

        return size;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        ByteBufferImExporter exporter = new ByteBufferImExporter(p_buffer);
        int size = m_chunk.sizeofObject();

        p_buffer.putLong(m_chunk.getID());
        exporter.exportObject(m_chunk);
        p_buffer.put((byte) (m_isReplicate ? 1 : 0));
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        ByteBufferImExporter importer = new ByteBufferImExporter(p_buffer);

        m_data = new DSByteArray(p_buffer.getLong(), p_buffer.getInt());
        importer.importObject(m_data);
        m_isReplicate = p_buffer.get() != 0;
    }
}
