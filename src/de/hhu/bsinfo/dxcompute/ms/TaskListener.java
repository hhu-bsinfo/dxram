
package de.hhu.bsinfo.dxcompute.ms;

/**
 * Listener interface to listen to common task events.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public interface TaskListener {
    /**
     * Gets called when the task is taken from the queue and is about to get executed.
     * @param p_task
     *            Task about to get executed.
     */
    void taskBeforeExecution(final Task p_task);

    /**
     * Gets called when the task execution has completed.
     * @param p_task
     *            Task that completed execution.
     */
    void taskCompleted(final Task p_task);
}
