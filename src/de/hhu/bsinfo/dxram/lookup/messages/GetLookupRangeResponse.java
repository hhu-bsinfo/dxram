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
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.ethnet.core.AbstractResponse;

/**
 * Response to a LookupRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class GetLookupRangeResponse extends AbstractResponse {

    // Attributes
    private LookupRange m_lookupRange;

    // Constructors

    /**
     * Creates an instance of LookupResponse
     */
    public GetLookupRangeResponse() {
        super();

        m_lookupRange = null;
    }

    /**
     * Creates an instance of LookupResponse
     *
     * @param p_request
     *     the corresponding LookupRequest
     * @param p_lookupRange
     *     the primary peer, backup peers and range
     */
    public GetLookupRangeResponse(final GetLookupRangeRequest p_request, final LookupRange p_lookupRange) {
        super(p_request, LookupMessages.SUBTYPE_GET_LOOKUP_RANGE_RESPONSE);

        m_lookupRange = p_lookupRange;
    }

    // Getters

    /**
     * Get lookupRange
     *
     * @return the LookupRange
     */
    public final LookupRange getLookupRange() {
        return m_lookupRange;
    }

    @Override
    protected final int getPayloadLength() {
        return m_lookupRange.sizeofObject();
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        final ByteBufferImExporter exporter = new ByteBufferImExporter(p_buffer);

        exporter.exportObject(m_lookupRange);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        final ByteBufferImExporter importer = new ByteBufferImExporter(p_buffer);

        m_lookupRange = new LookupRange();
        importer.importObject(m_lookupRange);
    }

}
