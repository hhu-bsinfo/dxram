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

package de.hhu.bsinfo.dxram.net.messages;

import de.hhu.bsinfo.ethnet.AbstractResponse;

import java.nio.ByteBuffer;

/**
 * Network response for running tests/benchmarks.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 04.04.2017
 */
public class NetworkTestResponse extends AbstractResponse{

    private byte[] m_data;


    /**
     * Creates an instance of NetworkTestResponse.
     * This constructor is used when receiving this message.
     */
    public NetworkTestResponse() {
        super();
    }


    /**
     * Creates an instance of StatusResponse.
     * This constructor is used when sending this message.
     * @param p_request
     *            the corresponding StatusRequest
     * @param p_payloadSize
     *            size of the payload
     */
    public NetworkTestResponse(final NetworkTestRequest p_request, final int p_payloadSize) {
        super(p_request, NetworkMessages.SUBTYPE_TEST_RESPONSE);

        m_data = new byte[p_payloadSize];
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_data.length);
        p_buffer.put(m_data);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int length = p_buffer.getInt();
        m_data = new byte[length];
        p_buffer.get(m_data, 0, length);
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + m_data.length * Byte.BYTES;
    }
}
