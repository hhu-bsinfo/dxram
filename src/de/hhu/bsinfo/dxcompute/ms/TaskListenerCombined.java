
package de.hhu.bsinfo.dxcompute.ms;

/**
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 19.10.2016
 */
public class TaskListenerCombined implements TaskListener {

    private TaskListenerBeforExecution m_beforeExecution;
    private TaskListenerTaskCompleted m_taskCompleted;

    public TaskListenerCombined(final TaskListenerBeforExecution p_beforeExecution, final TaskListenerTaskCompleted p_taskCompleted) {

        m_beforeExecution = p_beforeExecution;
        m_taskCompleted = p_taskCompleted;
    }

    @Override
    public void taskBeforeExecution(final Task p_task) {
        m_beforeExecution.taskBeforeExecution(p_task);
    }

    @Override
    public void taskCompleted(final Task p_task) {
        m_taskCompleted.taskCompleted(p_task);
    }
}
