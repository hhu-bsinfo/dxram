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

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.data.ChunkIDRanges;
import de.hhu.bsinfo.utils.serialization.ByteBufferImExporter;
import de.hhu.bsinfo.net.core.AbstractResponse;

/**
 * Response to the request sending the chunk id ranges of all migrated locally stored chunks.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class GetMigratedChunkIDRangesResponse extends AbstractResponse {
    private ChunkIDRanges m_chunkIDRanges = new ChunkIDRanges();

    /**
     * Creates an instance of GetMigratedChunkIDRangesResponse.
     * This constructor is used when receiving this message.
     */
    public GetMigratedChunkIDRangesResponse() {
        super();
    }

    /**
     * Creates an instance of GetMigratedChunkIDRangesResponse.
     * This constructor is used when sending this message.
     *
     * @param p_request
     *     the corresponding GetMigratedChunkIDRangesRequest
     * @param p_chunkIDs
     *     Chunk id ranges to send
     */
    public GetMigratedChunkIDRangesResponse(final GetMigratedChunkIDRangesRequest p_request, ChunkIDRanges p_chunkIDs) {
        super(p_request, ChunkMessages.SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_RESPONSE);

        m_chunkIDRanges = p_chunkIDs;
    }

    /**
     * Get the chunk id ranges from this message.
     *
     * @return Ranges of chunk ids from the remote node.
     */
    public ChunkIDRanges getChunkIDRanges() {
        return m_chunkIDRanges;
    }

    @Override
    protected final int getPayloadLength() {
        return m_chunkIDRanges.sizeofObject();
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        ByteBufferImExporter imExporter = new ByteBufferImExporter(p_buffer);

        imExporter.exportObject(m_chunkIDRanges);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        ByteBufferImExporter imExporter = new ByteBufferImExporter(p_buffer);

        imExporter.importObject(m_chunkIDRanges);
    }
}
