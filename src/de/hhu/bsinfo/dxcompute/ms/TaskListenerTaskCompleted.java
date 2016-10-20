package de.hhu.bsinfo.dxcompute.ms;

/**
 * Created by nothaas on 10/19/16.
 */
public interface TaskListenerTaskCompleted {
	/**
	 * Gets called when the task execution has completed.
	 *
	 * @param p_task Task that completed execution.
	 */
	void taskCompleted(final Task p_task);
}
