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

package de.hhu.bsinfo.dxram.lookup.messages;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;

/**
 * Response to a GetChunkIDRequest
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 */
public class GetChunkIDForNameserviceEntryResponse extends Response {

    // Attributes
    private long m_chunkID;

    // Constructors

    /**
     * Creates an instance of GetChunkIDResponse
     */
    public GetChunkIDForNameserviceEntryResponse() {
        super();

        m_chunkID = ChunkID.INVALID_ID;
    }

    /**
     * Creates an instance of GetChunkIDResponse
     *
     * @param p_request
     *         the request
     * @param p_chunkID
     *         the ChunkID
     */
    public GetChunkIDForNameserviceEntryResponse(final GetChunkIDForNameserviceEntryRequest p_request, final long p_chunkID) {
        super(p_request, LookupMessages.SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_RESPONSE);

        m_chunkID = p_chunkID;
    }

    // Getters

    /**
     * Get the ChunkID
     *
     * @return the ChunkID
     */
    public final long getChunkID() {
        return m_chunkID;
    }

    @Override
    protected final int getPayloadLength() {
        return Long.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeLong(m_chunkID);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_chunkID = p_importer.readLong(m_chunkID);
    }

}
