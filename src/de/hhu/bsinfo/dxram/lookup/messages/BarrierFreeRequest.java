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

import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Message to free an allocated barrier.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.05.2016
 */
public class BarrierFreeRequest extends AbstractRequest {
    private int m_barrierId;
    private boolean m_isReplicate;

    /**
     * Creates an instance of BarrierFreeRequest
     */
    public BarrierFreeRequest() {
        super();
    }

    /**
     * Creates an instance of BarrierFreeRequest
     *
     * @param p_destination
     *     the destination
     * @param p_barrierId
     *     Id of the barrier to free
     * @param p_isReplicate
     *     wether it is a replicate or not
     */
    public BarrierFreeRequest(final short p_destination, final int p_barrierId, final boolean p_isReplicate) {
        super(p_destination, DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_BARRIER_FREE_REQUEST);

        m_barrierId = p_barrierId;
        m_isReplicate = p_isReplicate;
    }

    /**
     * Get the barrier id to free.
     *
     * @return Barrier id.
     */
    public int getBarrierId() {
        return m_barrierId;
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
        p_buffer.putInt(m_barrierId);
        p_buffer.put((byte) (m_isReplicate ? 1 : 0));
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_barrierId = p_buffer.getInt();
        m_isReplicate = p_buffer.get() == (byte) 1;
    }
}
