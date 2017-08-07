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

import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;

/**
 * Response to a LookupRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.09.2012
 */
public class GetLookupRangeResponse extends Response {

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
     *         the corresponding LookupRequest
     * @param p_lookupRange
     *         the primary peer, backup peers and range
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
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.exportObject(m_lookupRange);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        if (m_lookupRange == null) {
            m_lookupRange = new LookupRange();
        }
        p_importer.importObject(m_lookupRange);
    }

}
