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

import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;

/**
 * Response to the request sending the chunk id ranges of all locally stored chunks.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class GetLocalChunkIDRangesResponse extends Response {
    private ChunkIDRanges m_chunkIDRanges = new ChunkIDRanges();

    /**
     * Creates an instance of StatusResponse.
     * This constructor is used when receiving this message.
     */
    public GetLocalChunkIDRangesResponse() {
        super();
    }

    /**
     * Creates an instance of StatusResponse.
     * This constructor is used when sending this message.
     *
     * @param p_request
     *         the corresponding StatusRequest
     * @param p_chunkIDRanges
     *         Chunk id ranges to send
     */
    public GetLocalChunkIDRangesResponse(final GetLocalChunkIDRangesRequest p_request, ChunkIDRanges p_chunkIDRanges) {
        super(p_request, ChunkMessages.SUBTYPE_GET_LOCAL_CHUNKID_RANGES_RESPONSE);

        m_chunkIDRanges = p_chunkIDRanges;
    }

    /**
     * Get the chunk id ranges from this message.
     *
     * @return List of chunk id ranges from the remote node.
     */
    public ChunkIDRanges getChunkIDRanges() {
        return m_chunkIDRanges;
    }

    @Override
    protected final int getPayloadLength() {
        return m_chunkIDRanges.sizeofObject();
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.exportObject(m_chunkIDRanges);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        p_importer.importObject(m_chunkIDRanges);
    }
}
