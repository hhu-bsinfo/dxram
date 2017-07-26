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

import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.net.core.Response;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * Response to a GetMetadataSummaryRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.10.2016
 */
public class GetMetadataSummaryResponse extends Response {

    // Attributes
    private String m_summary;

    // Constructors

    /**
     * Creates an instance of GetMetadataSummaryResponse
     */
    public GetMetadataSummaryResponse() {
        super();
    }

    /**
     * Creates an instance of SendBackupsMessage
     *
     * @param p_request
     *         the corresponding GetMetadataSummaryRequest
     * @param p_summary
     *         the metadata summary
     */
    public GetMetadataSummaryResponse(final GetMetadataSummaryRequest p_request, final String p_summary) {
        super(p_request, LookupMessages.SUBTYPE_GET_METADATA_SUMMARY_RESPONSE);

        m_summary = p_summary;
    }

    // Getters

    /**
     * Get metadata summary
     *
     * @return the metadata summary
     */
    public final String getMetadataSummary() {
        return m_summary;
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofString(m_summary);
    }

    // Methods
    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeString(m_summary);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_summary = p_importer.readString(m_summary);
    }

}
