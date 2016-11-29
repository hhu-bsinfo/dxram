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

package de.hhu.bsinfo.dxram.lookup.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Message to get the current status of an active barrier.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 06.05.2016
 */
public class BarrierGetStatusResponse extends AbstractResponse {
    private int m_barrierId;
    private short[] m_barrierStatus;

    /**
     * Creates an instance of BarrierGetStatusResponse.
     * This constructor is used when receiving this message.
     */
    public BarrierGetStatusResponse() {
        super();
    }

    /**
     * Creates an instance of BarrierGetStatusResponse.
     * This constructor is used when sending this message.
     *
     * @param p_request
     *     The request for the response
     * @param p_barrierStatus
     *     Status of the barrier
     */
    public BarrierGetStatusResponse(final BarrierGetStatusRequest p_request, final short[] p_barrierStatus) {
        super(p_request, LookupMessages.SUBTYPE_BARRIER_STATUS_RESPONSE);

        m_barrierId = p_request.getBarrierId();
        m_barrierStatus = p_barrierStatus;
    }

    /**
     * Get the id of the barrier
     *
     * @return Barrier id.
     */
    public int getBarrierId() {
        return m_barrierId;
    }

    /**
     * Get the barrier status.
     * First value is the number of signed on peers.
     *
     * @return Barrier status.
     */
    public short[] getBarrierStatus() {
        return m_barrierStatus;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + Integer.BYTES + m_barrierStatus.length * Short.BYTES;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_barrierId);
        p_buffer.putInt(m_barrierStatus.length);
        for (short barrierStatus : m_barrierStatus) {
            p_buffer.putShort(barrierStatus);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_barrierId = p_buffer.getInt();
        m_barrierStatus = new short[p_buffer.getInt()];
        for (int i = 0; i < m_barrierStatus.length; i++) {
            m_barrierStatus[i] = p_buffer.getShort();
        }
    }
}
