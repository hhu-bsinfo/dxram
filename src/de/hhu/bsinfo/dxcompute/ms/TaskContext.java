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

package de.hhu.bsinfo.dxcompute.ms;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;

/**
 * Context for a task payload to give access to various data and interfaces.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 12.10.2016
 */
public class TaskContext {

    private TaskContextData m_ctxData;
    private TaskSignalInterface m_signalInterface;
    private DXRAMServiceAccessor m_dxramAccessor;

    TaskContext(final TaskContextData p_ctxData, final TaskSignalInterface p_signalInterface, final DXRAMServiceAccessor p_dxramAccessor) {
        m_ctxData = p_ctxData;
        m_signalInterface = p_signalInterface;
        m_dxramAccessor = p_dxramAccessor;
    }

    /**
     * Get the data/parameter of the task context.
     * @return TaskContextData
     */
    public TaskContextData getCtxData() {
        return m_ctxData;
    }

    /**
     * Get the signal interface for this task.
     * @return TaskSingalInterface
     */
    public TaskSignalInterface getSignalInterface() {
        return m_signalInterface;
    }

    /**
     * Get the DXRAM service accessor to access services in the task.
     * @return DXRAMServiceAccessor
     */
    public DXRAMServiceAccessor getDXRAMServiceAccessor() {
        return m_dxramAccessor;
    }
}
