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

import de.hhu.bsinfo.ethnet.core.AbstractResponse;

/**
 * Response to a GetMetadataSummaryRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.10.2016
 */
public class GetMetadataSummaryResponse extends AbstractResponse {

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
     *     the corresponding GetMetadataSummaryRequest
     * @param p_summary
     *     the metadata summary
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
        return m_summary.getBytes().length + Integer.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_summary.getBytes().length);
        p_buffer.put(m_summary.getBytes());
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int length;
        byte[] data;

        length = p_buffer.getInt();
        data = new byte[length];
        p_buffer.get(data);
        m_summary = new String(data);
    }

}
