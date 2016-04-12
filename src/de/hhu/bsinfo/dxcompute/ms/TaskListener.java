
package de.hhu.bsinfo.dxcompute.ms;

public interface TaskListener {
	public void taskBeforeExecution(final Task p_task);

	public void taskCompleted(final Task p_task);
}
