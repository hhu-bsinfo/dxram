/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Reponse message to the create request.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class CreateResponse extends Response {

    private long[] m_chunkIDs;

    /**
     * Creates an instance of CreateResponse.
     * This constructor is used when receiving this message.
     */
    public CreateResponse() {
        super();
    }

    /**
     * Creates an instance of CreateResponse.
     * This constructor is used when sending this message.
     * Make sure to include all the chunks with IDs from the request in the correct order. If a chunk does
     * not exist, no data and a length of 0 indicates this situation.
     *
     * @param p_request
     *         the corresponding GetRequest
     * @param p_chunkIDs
     *         The chunk IDs requested
     */
    public CreateResponse(final CreateRequest p_request, final long... p_chunkIDs) {
        super(p_request, ChunkMessages.SUBTYPE_CREATE_RESPONSE);
        m_chunkIDs = p_chunkIDs;
    }

    /**
     * Get the chunk IDs of the created chunks.
     *
     * @return ChunkIDs.
     */
    public final long[] getChunkIDs() {
        return m_chunkIDs;
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofLongArray(m_chunkIDs);
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeLongArray(m_chunkIDs);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_chunkIDs = p_importer.readLongArray(m_chunkIDs);
    }
}
