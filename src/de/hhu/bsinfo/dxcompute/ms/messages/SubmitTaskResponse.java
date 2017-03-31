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

package de.hhu.bsinfo.dxcompute.ms.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response by the remote master to the submitted task.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class SubmitTaskResponse extends AbstractResponse {
    private short m_assignedComputeGroupId;
    private int m_assignedPayloadId;

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
     * @param p_request
     *            request to respond to.
     * @param p_assignedComputeGroupId
     *            The compute group this task was assigned to.
     * @param p_assignedPayloadId
     *            The payload id assigned by the master of the compute group.
     */
    public SubmitTaskResponse(final SubmitTaskRequest p_request, final short p_assignedComputeGroupId, final int p_assignedPayloadId) {
        super(p_request, MasterSlaveMessages.SUBTYPE_SUBMIT_TASK_RESPONSE);

        m_assignedComputeGroupId = p_assignedComputeGroupId;
        m_assignedPayloadId = p_assignedPayloadId;
    }

    /**
     * Get the compute group id of the compute group the task was assigned to.
     * @return Compute group id.
     */
    public short getAssignedComputeGroupId() {
        return m_assignedComputeGroupId;
    }

    /**
     * Get the payload id that got assigned to the task by the master of the group.
     * @return Payload id.
     */
    public int getAssignedPayloadId() {
        return m_assignedPayloadId;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putShort(m_assignedComputeGroupId);
        p_buffer.putInt(m_assignedPayloadId);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_assignedComputeGroupId = p_buffer.getShort();
        m_assignedPayloadId = p_buffer.getInt();
    }

    @Override
    protected final int getPayloadLength() {
        return Short.BYTES + Integer.BYTES;
    }
}
