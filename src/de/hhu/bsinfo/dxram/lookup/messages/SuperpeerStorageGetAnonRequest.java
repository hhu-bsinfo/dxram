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

import de.hhu.bsinfo.dxram.data.ChunkAnon;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request to get data from the superpeer storage (anonymous chunk).
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 04.04.2017
 */
public class SuperpeerStorageGetAnonRequest extends AbstractRequest {
    // the chunk is stored for the sender of the request
    // to write the incoming data of the response to it
    // the requesting IDs are taken from the chunk
    private ChunkAnon m_chunk;
    // this is only used when receiving the request
    private int m_storageID;

    /**
     * Creates an instance of SuperpeerStorageGetRequest.
     * This constructor is used when receiving this message.
     */
    public SuperpeerStorageGetAnonRequest() {
        super();
    }

    /**
     * Creates an instance of SuperpeerStorageGetRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *     the destination node id.
     * @param p_chunk
     *     Anonymous chunk with the ID of the chunk to get.
     */
    public SuperpeerStorageGetAnonRequest(final short p_destination, final ChunkAnon p_chunk) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_ANON_REQUEST);

        m_chunk = p_chunk;
    }

    /**
     * Get the storage id.
     *
     * @return Storage id.
     */
    public int getStorageID() {
        return m_storageID;
    }

    /**
     * Get the chunk stored with this request.
     * This is used to write the received data to the provided object to avoid
     * using multiple buffers.
     *
     * @return Chunk to store data to when the response arrived.
     */
    public ChunkAnon getDataStructure() {
        return m_chunk;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt((int) m_chunk.getID());
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_storageID = p_buffer.getInt();
    }
}
