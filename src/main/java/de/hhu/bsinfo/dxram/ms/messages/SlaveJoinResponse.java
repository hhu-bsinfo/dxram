/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.ms.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;

/**
 * Response of the master to a join request by a slave.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class SlaveJoinResponse extends Response {
    private int m_executionBarrierId = BarrierID.INVALID_ID;
    private byte m_status;

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
     *
     * @param p_request
     *         request to respond to.
     * @param p_executionBarrierId
     *         The id of the barrier to sync for execution of a task
     */
    public SlaveJoinResponse(final SlaveJoinRequest p_request, final int p_executionBarrierId, final byte p_status) {
        super(p_request, MasterSlaveMessages.SUBTYPE_SLAVE_JOIN_RESPONSE);
        m_executionBarrierId = p_executionBarrierId;
        m_status = p_status;
    }

    /**
     * Get the barrier id used for the execution barrier to sync slaves to the master.
     *
     * @return Execution barrier id.
     */
    public int getExecutionBarrierId() {
        return m_executionBarrierId;
    }

    /**
     * Get the status
     *
     * @return the status
     */
    public int getStatus() {
        return m_status;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_executionBarrierId);
        p_exporter.writeByte(m_status);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_executionBarrierId = p_importer.readInt(m_executionBarrierId);
        m_status = p_importer.readByte(m_status);
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + Byte.BYTES;
    }
}
