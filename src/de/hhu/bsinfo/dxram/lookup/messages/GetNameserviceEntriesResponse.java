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

import de.hhu.bsinfo.net.core.AbstractResponse;

/**
 * Response to a GetMappingCountRequest
 *
 * @author Florian Klein, florian.klein@hhu.de, 26.03.2015
 */
public class GetNameserviceEntriesResponse extends AbstractResponse {

    private byte[] m_entries;

    // Constructors

    /**
     * Creates an instance of GetNameserviceEntriesResponse
     */
    public GetNameserviceEntriesResponse() {
        super();
    }

    /**
     * Creates an instance of GetNameserviceEntriesResponse
     *
     * @param p_request
     *     the request
     * @param p_entries
     *     the count
     */
    public GetNameserviceEntriesResponse(final GetNameserviceEntriesRequest p_request, final byte[] p_entries) {
        super(p_request, LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRIES_RESPONSE);

        m_entries = p_entries;
    }

    // Getters

    /**
     * Get the entries.
     *
     * @return Entries
     */
    public byte[] getEntries() {
        return m_entries;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + m_entries.length;
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        if (m_entries.length == 0) {
            p_buffer.putInt(0);
        } else {
            p_buffer.putInt(m_entries.length);
            p_buffer.put(m_entries);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int length = p_buffer.getInt();
        if (length != 0) {
            m_entries = new byte[length];
            p_buffer.get(m_entries);
        } else {
            m_entries = new byte[0];
        }
    }

}
