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

import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.net.core.AbstractResponse;

/**
 * Response by the remote master to the submitted task.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class SubmitTaskResponse extends AbstractResponse {
    private short m_assignedComputeGroupId;
    private int m_assignedPayloadId;
    private byte m_status;

    /**
     * Creates an instance of SubmitTaskResponse.
     * This constructor is used when receiving this message.
     */
    public SubmitTaskResponse() {
        super();
    }

    /**
     * Creates an instance of SubmitTaskResponse.
     * This constructor is used when sending this message.
     *
     * @param p_request
     *         request to respond to.
     * @param p_assignedComputeGroupId
     *         The compute group this task was assigned to.
     * @param p_assignedPayloadId
     *         The payload id assigned by the master of the compute group.
     */
    public SubmitTaskResponse(final SubmitTaskRequest p_request, final short p_assignedComputeGroupId, final int p_assignedPayloadId, final byte p_status) {
        super(p_request, MasterSlaveMessages.SUBTYPE_SUBMIT_TASK_RESPONSE);
        m_assignedComputeGroupId = p_assignedComputeGroupId;
        m_assignedPayloadId = p_assignedPayloadId;
        m_status = p_status;
    }

    /**
     * Get the compute group id of the compute group the task was assigned to.
     *
     * @return Compute group id.
     */
    public short getAssignedComputeGroupId() {
        return m_assignedComputeGroupId;
    }

    /**
     * Get the payload id that got assigned to the task by the master of the group.
     *
     * @return Payload id.
     */
    public int getAssignedPayloadId() {
        return m_assignedPayloadId;
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
        p_exporter.writeShort(m_assignedComputeGroupId);
        p_exporter.writeInt(m_assignedPayloadId);
        p_exporter.writeByte(m_status);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_assignedComputeGroupId = p_importer.readShort(m_assignedComputeGroupId);
        m_assignedPayloadId = p_importer.readInt(m_assignedPayloadId);
        m_status = p_importer.readByte(m_status);
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES + Integer.BYTES + Byte.BYTES;
    }
}
