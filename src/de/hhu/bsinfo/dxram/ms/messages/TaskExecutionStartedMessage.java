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
import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;

/**
 * Notify all remote listeners about a task that started execution.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class TaskExecutionStartedMessage extends AbstractMessage {
    private int m_taskPayloadId;

    /**
     * Creates an instance of TaskRemoteCallbackMessage.
     * This constructor is used when receiving this message.
     */
    public TaskExecutionStartedMessage() {
        super();
    }

    /**
     * Creates an instance of TaskRemoteCallbackMessage.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination node id.
     * @param p_taskPayloadId
     *         Id of the task that started execution.
     */
    public TaskExecutionStartedMessage(final short p_destination, final int p_taskPayloadId) {
        super(p_destination, DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_TASK_EXECUTION_STARTED_MESSAGE);

        m_taskPayloadId = p_taskPayloadId;
    }

    /**
     * Id of the task that started execution.
     *
     * @return Id of the task.
     */
    public int getTaskPayloadId() {
        return m_taskPayloadId;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_taskPayloadId);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_taskPayloadId = p_importer.readInt(m_taskPayloadId);
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES;
    }
}
