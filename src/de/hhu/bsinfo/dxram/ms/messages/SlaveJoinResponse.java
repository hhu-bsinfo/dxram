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

package de.hhu.bsinfo.dxram.ms.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response of the master to a join request by a slave.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class SlaveJoinResponse extends AbstractResponse {
    private int m_executionBarrierId = BarrierID.INVALID_ID;

    /**
     * Creates an instance of SlaveJoinResponse.
     * This constructor is used when receiving this message.
     */
    public SlaveJoinResponse() {
        super();
    }

    /**
     * Creates an instance of SlaveJoinResponse.
     * This constructor is used when sending this message.
     * @param p_request
     *            request to respond to.
     * @param p_executionBarrierId
     *            The id of the barrier to sync for execution of a task
     */
    public SlaveJoinResponse(final SlaveJoinRequest p_request, final int p_executionBarrierId) {
        super(p_request, MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_RESPONSE);

        m_executionBarrierId = p_executionBarrierId;
    }

    /**
     * Get the barrier id used for the execution barrier to sync slaves to the master.
     * @return Execution barrier id.
     */
    public int getExecutionBarrierId() {
        return m_executionBarrierId;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_executionBarrierId);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_executionBarrierId = p_buffer.getInt();
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES;
    }
}
