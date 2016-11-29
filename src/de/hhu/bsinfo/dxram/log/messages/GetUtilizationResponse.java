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

package de.hhu.bsinfo.dxram.log.messages;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to a GetUtilizationRequest
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 12.04.2016
 */
public class GetUtilizationResponse extends AbstractResponse {

    // Attributes
    private String m_utilization;

    // Constructors

    /**
     * Creates an instance of GetUtilizationResponse
     */
    public GetUtilizationResponse() {
        super();

        m_utilization = null;
    }

    /**
     * Creates an instance of GetUtilizationResponse
     *
     * @param p_request
     *     the corresponding GetUtilizationRequest
     * @param p_utilization
     *     the utilization as a String
     */
    public GetUtilizationResponse(final GetUtilizationRequest p_request, final String p_utilization) {
        super(p_request, LogMessages.SUBTYPE_GET_UTILIZATION_RESPONSE);

        m_utilization = p_utilization;
    }

    // Getters

    /**
     * Get the utilization
     *
     * @return the utilization
     */
    public final String getUtilization() {
        if (m_utilization != null) {
            return m_utilization;
        } else {
            return "Given node is not a peer";
        }
    }

    @Override
    protected final int getPayloadLength() {
        if (m_utilization != null) {
            return Integer.BYTES + m_utilization.getBytes(StandardCharsets.UTF_8).length;
        } else {
            return Integer.BYTES;
        }
    }

    // Methods
    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        if (m_utilization != null) {
            byte[] data = m_utilization.getBytes(StandardCharsets.UTF_8);

            p_buffer.putInt(data.length);
            p_buffer.put(data);
        } else {
            p_buffer.putInt(0);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int length;
        byte[] data;

        length = p_buffer.getInt();
        if (length > 0) {
            data = new byte[length];
            p_buffer.get(data);
            m_utilization = new String(data, StandardCharsets.UTF_8);
        }
    }

}
