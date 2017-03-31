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

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a GetMappingCountRequest
 *
 * @author Florian Klein, florian.klein@hhu.de, 26.03.2015
 */
public class GetNameserviceEntryCountResponse extends AbstractResponse {

    // Attributes
    private int m_count;

    // Constructors

    /**
     * Creates an instance of GetMappingCountResponse
     */
    public GetNameserviceEntryCountResponse() {
        super();

        m_count = 0;
    }

    /**
     * Creates an instance of GetMappingCountResponse
     *
     * @param p_request
     *     the request
     * @param p_count
     *     the count
     */
    public GetNameserviceEntryCountResponse(final GetNameserviceEntryCountRequest p_request, final int p_count) {
        super(p_request, LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_RESPONSE);

        m_count = p_count;
    }

    // Getters

    /**
     * Get the count
     *
     * @return the count
     */
    public final int getCount() {
        return m_count;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_count);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_count = p_buffer.getInt();
    }

}
