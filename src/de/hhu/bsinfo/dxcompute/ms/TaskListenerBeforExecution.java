package de.hhu.bsinfo.dxcompute.ms;

/**
 * Created by nothaas on 10/19/16.
 */
public interface TaskListenerBeforExecution {
	/**
	 * Gets called when the task is taken from the queue and is about to get executed.
	 *
	 * @param p_task Task about to get executed.
	 */
	void taskBeforeExecution(final Task p_task);
}
