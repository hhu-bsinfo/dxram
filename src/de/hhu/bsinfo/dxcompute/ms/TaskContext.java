
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
