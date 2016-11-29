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

package de.hhu.bsinfo.dxcompute.ms.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxcompute.DXComputeMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;

/**
 * Notify remote listeners that execution of a submitted task has finished.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class TaskExecutionFinishedMessage extends AbstractMessage {
    private int m_taskPayloadId;
    private int[] m_executionReturnCodes;

    /**
     * Creates an instance of TaskRemoteCallbackMessage.
     * This constructor is used when receiving this message.
     */
    public TaskExecutionFinishedMessage() {
        super();
    }

    /**
     * Creates an instance of TaskRemoteCallbackMessage.
     * This constructor is used when sending this message.
     * @param p_destination
     *            the destination node id.
     * @param p_taskPayloadId
     *            Payload id of the task that finished
     * @param p_executionReturnCodes
     *            Return codes of all slaves that executed the task (Indexable by slave id).
     */
    public TaskExecutionFinishedMessage(final short p_destination, final int p_taskPayloadId, final int[] p_executionReturnCodes) {
        super(p_destination, DXComputeMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_FINISHED_MESSAGE);

        m_taskPayloadId = p_taskPayloadId;
        m_executionReturnCodes = p_executionReturnCodes;
    }

    /**
     * Get the payload if of the task that finished execution.
     * @return Payload id of the finished task.
     */
    public int getTaskPayloadId() {
        return m_taskPayloadId;
    }

    /**
     * Get the return codes of all slaves that executed the task (Indexable by slave id).
     * @return Return codes.
     */
    public int[] getExecutionReturnCodes() {
        return m_executionReturnCodes;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_taskPayloadId);
        p_buffer.putInt(m_executionReturnCodes.length);
        for (int executionReturnCode : m_executionReturnCodes) {
            p_buffer.putInt(executionReturnCode);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        m_taskPayloadId = p_buffer.getInt();
        int size = p_buffer.getInt();
        m_executionReturnCodes = new int[size];
        for (int i = 0; i < size; i++) {
            m_executionReturnCodes[i] = p_buffer.getInt();
        }
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + Integer.BYTES + Integer.BYTES * m_executionReturnCodes.length;
    }
}
