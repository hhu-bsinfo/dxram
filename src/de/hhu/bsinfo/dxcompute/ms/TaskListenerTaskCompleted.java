
package de.hhu.bsinfo.dxcompute.ms;

/**
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 19.10.2016
 */
public interface TaskListenerTaskCompleted {
    /**
     * Gets called when the task execution has completed.
     * @param p_task
     *            Task that completed execution.
     */
    void taskCompleted(final Task p_task);
}
