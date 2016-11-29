/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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
import java.util.ArrayList;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to the request sending the chunk id ranges of all migrated locally stored chunks.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class GetMigratedChunkIDRangesResponse extends AbstractResponse {
    private ArrayList<Long> m_chunkIDRanges;

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
     * @param p_chunkIDRanges
     *     Chunk id ranges to send
     */
    public GetMigratedChunkIDRangesResponse(final GetMigratedChunkIDRangesRequest p_request, final ArrayList<Long> p_chunkIDRanges) {
        super(p_request, ChunkMessages.SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_RESPONSE);

        m_chunkIDRanges = p_chunkIDRanges;
    }

    /**
     * Get the chunk id ranges from this message.
     *
     * @return List of chunk id ranges from the remote node.
     */
    public ArrayList<Long> getChunkIDRanges() {
        return m_chunkIDRanges;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + Long.BYTES * m_chunkIDRanges.size();
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_chunkIDRanges.size());
        for (int i = 0; i < m_chunkIDRanges.size(); i++) {
            p_buffer.putLong(m_chunkIDRanges.get(i));
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int size = p_buffer.getInt();
        m_chunkIDRanges = new ArrayList<Long>(size);
        for (int i = 0; i < size; i++) {
            m_chunkIDRanges.add(p_buffer.getLong());
        }
    }
}
