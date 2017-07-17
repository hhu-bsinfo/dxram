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
import de.hhu.bsinfo.dxram.ms.TaskContextData;
import de.hhu.bsinfo.dxram.ms.TaskScript;
import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.net.core.AbstractRequest;

/**
 * Request to execute a task script on another slave compute node.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class ExecuteTaskScriptRequest extends AbstractRequest {

    private int m_barrierIdentifier = -1;
    private TaskContextData m_ctxData;
    private TaskScript m_script;

    /**
     * Creates an instance of ExecuteTaskScriptRequest.
     * This constructor is used when receiving this message.
     */
    public ExecuteTaskScriptRequest() {
        super();
    }

    /**
     * Creates an instance of ExecuteTaskScriptRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination node id.
     * @param p_barrierIdentifier
     *         Barrier identifier for synchronization after done executing.
     * @param p_script
     *         TaskScript to execute.
     */
    public ExecuteTaskScriptRequest(final short p_destination, final int p_barrierIdentifier, final TaskContextData p_ctxData, final TaskScript p_script) {
        super(p_destination, DXRAMMessageTypes.MASTERSLAVE_MESSAGES_TYPE, MasterSlaveMessages.SUBTYPE_EXECUTE_TASK_REQUEST);
        m_barrierIdentifier = p_barrierIdentifier;
        m_ctxData = p_ctxData;
        m_script = p_script;
    }

    /**
     * Get the barrier identifier to use after finishing execution and syncing to the master.
     *
     * @return Barrier identifier for sync.
     */
    public int getBarrierIdentifier() {
        return m_barrierIdentifier;
    }

    /**
     * Get the context data for the task to execute.
     *
     * @return Context data.
     */
    public TaskContextData getTaskContextData() {
        return m_ctxData;
    }

    /**
     * Get the task script to execute.
     *
     * @return TaskScript
     */
    public TaskScript getTaskScript() {
        return m_script;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_barrierIdentifier);
        p_exporter.exportObject(m_ctxData);
        p_exporter.exportObject(m_script);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_barrierIdentifier = p_importer.readInt(m_barrierIdentifier);
        if (m_ctxData == null) {
            m_ctxData = new TaskContextData();
        }
        p_importer.importObject(m_ctxData);
        if (m_script == null) {
            m_script = new TaskScript();
        }
        p_importer.importObject(m_script);
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + m_ctxData.sizeofObject() + m_script.sizeofObject();
    }
}
