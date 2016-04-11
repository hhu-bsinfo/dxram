
package de.hhu.bsinfo.dxcompute.ms;

public interface TaskListener {
	public void taskBeforeExecution(final Task2 p_task);

	public void taskCompleted(final Task2 p_task);
}
