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

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request to allocate/create a new barrier.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.05.2016
 */
public class BarrierAllocRequest extends AbstractRequest {
    private int m_size;
    private boolean m_isReplicate;

    /**
     * Creates an instance of BarrierAllocRequest
     */
    public BarrierAllocRequest() {
        super();
    }

    /**
     * Creates an instance of LookupRequest
     *
     * @param p_destination
     *     the destination
     * @param p_size
     *     size of the barrier
     * @param p_isReplicate
     *     wether it is a replicate or not
     */
    public BarrierAllocRequest(final short p_destination, final int p_size, final boolean p_isReplicate) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_ALLOC_REQUEST);

        m_size = p_size;
        m_isReplicate = p_isReplicate;
    }

    /**
     * Get the barrier size;
     *
     * @return Barrier size
     */
    public int getBarrierSize() {
        return m_size;
    }

    /**
     * Returns if it is a replicate or not.
     *
     * @return True if it is a replicate, false otherwise.
     */
    public boolean isReplicate() {
        return m_isReplicate;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + Byte.BYTES;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_size);
        p_buffer.put((byte) (m_isReplicate ? 1 : 0));
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_size = p_buffer.getInt();
        m_isReplicate = p_buffer.get() == (byte) 1;
    }
}
