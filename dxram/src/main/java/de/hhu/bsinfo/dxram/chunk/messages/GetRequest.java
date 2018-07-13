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

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Request for getting a chunk from a remote node. The size of a chunk is known prior fetching the data
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public class GetRequest extends Request {

    // the chunk is stored for the sender of the request
    // to write the incoming data of the response to it
    // the requesting IDs are taken from the chunk
    private DataStructure[] m_chunks;
    // this is only used when receiving the request
    private long[] m_chunkIDs;

    /**
     * Creates an instance of GetRequest.
     * This constructor is used when receiving this message.
     */
    public GetRequest() {
        super();
    }

    /**
     * Creates an instance of GetRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination node id.
     * @param p_chunks
     *         Chunks with the ID of the chunk data to get.
     */
    public GetRequest(final short p_destination, final DataStructure... p_chunks) {
        super(p_destination, DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_GET_REQUEST);
        m_chunks = p_chunks;
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
    public DataStructure[] getChunks() {
        return m_chunks;
    }

    @Override
    protected final int getPayloadLength() {
        if (m_chunks != null) {
            return ObjectSizeUtil.sizeofCompactedNumber(m_chunks.length) + Long.BYTES * m_chunks.length;
        } else {
            return ObjectSizeUtil.sizeofCompactedNumber(m_chunkIDs.length) + Long.BYTES * m_chunkIDs.length;
        }
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeCompactNumber(m_chunks.length);
        for (DataStructure chunk : m_chunks) {
            p_exporter.writeLong(chunk.getID());
        }
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_chunkIDs = p_importer.readLongArray(m_chunkIDs);
    }
}
