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
import de.hhu.bsinfo.dxcompute.ms.TaskContextData;
import de.hhu.bsinfo.dxcompute.ms.TaskPayload;
import de.hhu.bsinfo.dxcompute.ms.TaskPayloadManager;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.ethnet.AbstractRequest;

/**
 * Request to execute a task on another slave compute node.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class ExecuteTaskRequest extends AbstractRequest {

    private int m_barrierIdentifier = -1;
    private TaskContextData m_ctxData;
    private TaskPayload m_task;

    /**
     * Creates an instance of ExecuteTaskRequest.
     * This constructor is used when receiving this message.
     */
    public ExecuteTaskRequest() {
        super();
    }

    /**
     * Creates an instance of ExecuteTaskRequest.
     * This constructor is used when sending this message.
     * @param p_destination
     *            the destination node id.
     * @param p_barrierIdentifier
     *            Barrier identifier for synchronization after done executing.
     * @param p_task
     *            Task to execute.
     */
    public ExecuteTaskRequest(final short p_destination, final int p_barrierIdentifier, final TaskContextData p_ctxData, final TaskPayload p_task) {
        super(p_destination, DXComputeMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_REQUEST);
        m_barrierIdentifier = p_barrierIdentifier;
        m_ctxData = p_ctxData;
        m_task = p_task;
    }

    /**
     * Get the barrier identifier to use after finishing execution and syncing to the master.
     * @return Barrier identifier for sync.
     */
    public int getBarrierIdentifier() {
        return m_barrierIdentifier;
    }

    /**
     * Get the context data for the task to execute.
     * @return Context data.
     */
    public TaskContextData getTaskContextData() {
        return m_ctxData;
    }

    /**
     * Get the task payload to execute.
     * @return Task payload.
     */
    public TaskPayload getTaskPayload() {
        return m_task;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);

        p_buffer.putInt(m_barrierIdentifier);
        exporter.exportObject(m_ctxData);
        p_buffer.putShort(m_task.getTypeId());
        p_buffer.putShort(m_task.getSubtypeId());
        exporter.exportObject(m_task);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);

        m_barrierIdentifier = p_buffer.getInt();
        m_ctxData = new TaskContextData();
        importer.importObject(m_ctxData);
        short type = p_buffer.getShort();
        short subtype = p_buffer.getShort();
        m_task = TaskPayloadManager.createInstance(type, subtype);
        importer.importObject(m_task);
    }

    @Override
    protected final int getPayloadLength() {
        return 2 * Short.BYTES + Integer.BYTES + m_ctxData.sizeofObject() + m_task.sizeofObject();
    }
}
