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

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * Notify remote listeners that execution of a submitted task has finished.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class TaskExecutionFinishedMessage extends Message {
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
     *
     * @param p_destination
     *         the destination node id.
     * @param p_taskPayloadId
     *         Payload id of the task that finished
     * @param p_executionReturnCodes
     *         Return codes of all slaves that executed the task (Indexable by slave id).
     */
    public TaskExecutionFinishedMessage(final short p_destination, final int p_taskPayloadId, final int[] p_executionReturnCodes) {
        super(p_destination, DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_FINISHED_MESSAGE);

        m_taskPayloadId = p_taskPayloadId;
        m_executionReturnCodes = p_executionReturnCodes;
    }

    /**
     * Get the payload if of the task that finished execution.
     *
     * @return Payload id of the finished task.
     */
    public int getTaskPayloadId() {
        return m_taskPayloadId;
    }

    /**
     * Get the return codes of all slaves that executed the task (Indexable by slave id).
     *
     * @return Return codes.
     */
    public int[] getExecutionReturnCodes() {
        return m_executionReturnCodes;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_taskPayloadId);
        p_exporter.writeIntArray(m_executionReturnCodes);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_taskPayloadId = p_importer.readInt(m_taskPayloadId);
        m_executionReturnCodes = p_importer.readIntArray(m_executionReturnCodes);
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + ObjectSizeUtil.sizeofIntArray(m_executionReturnCodes);
    }
}
