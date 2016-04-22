
package de.hhu.bsinfo.dxcompute.ms;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;

/**
 * A task that can be submitted to a master-slave compute group.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class Task {

	private AbstractTaskPayload m_payload;
	private String m_name;
	private short m_nodeIdSubmitted;
	private ArrayList<TaskListener> m_completionListeners = new ArrayList<TaskListener>();
	protected DXRAMServiceAccessor m_serviceAccessor;

	/**
	 * Constructor
	 * @param p_payload
	 *            Payload for that task containing the code and data to execute.
	 * @param p_name
	 *            Name of the task (debug only).
	 */
	public Task(final AbstractTaskPayload p_payload, final String p_name) {
		m_payload = p_payload;
		m_name = p_name;
	}

	/**
	 * Name of the task (debugging purpose only).
	 * @return Name of the task
	 */
	public String getName() {
		return m_name;
	}

	/**
	 * Get the node id which submitted the task.
	 * @return Node id that submitted this task.
	 */
	public short getNodeIdSubmitted() {
		return m_nodeIdSubmitted;
	}

	/**
	 * Get the id of the compute group this task was assigned to.
	 * @return Compute group id the task is assigend to.
	 */
	public int getComputeGroupId() {
		return m_payload.getComputeGroupId();
	}

	/**
	 * Get the id assigned by the master node of this task.
	 * @return Id of this task.
	 */
	public long getTaskId() {
		return m_payload.getPayloadId();
	}

	/**
	 * Check if execution of the task has completed.
	 * @return True if execution completed, false otherwise.
	 */
	public boolean hasTaskExecutionCompleted() {
		return m_payload.getExecutionReturnCodes().length > 0;
	}

	/**
	 * Get the node ids of the slaves executing this task (available only if execution has started already).
	 * @return List of slaves executing this task.
	 */
	public short[] getSlaveNodeIdsExecutingTask() {
		return m_payload.getSlaveNodeIds();
	}

	/**
	 * Get the return codes of all slaves which finished execution of the task.
	 * @return If execution has finished, return code of each slave, empty array otherwise.
	 */
	public int[] getExecutionReturnCodes() {
		return m_payload.getExecutionReturnCodes();
	}

	/**
	 * Register a TaskListener for this task.
	 * @param p_listener
	 *            Listener to register.
	 */
	public void registerTaskListener(final TaskListener p_listener) {
		m_completionListeners.add(p_listener);
	}

	@Override
	public String toString() {
		return "Task[" + m_name + "][" + m_nodeIdSubmitted + "]: " + m_payload;
	}

	/**
	 * Get the payload coupled to this task.
	 * @return TaskPayload
	 */
	AbstractTaskPayload getPayload() {
		return m_payload;
	}

	/**
	 * Set the DXRAM service accessor.
	 * @param p_accessor
	 *            Accessor
	 */
	void setDXRAMServiceAccessor(final DXRAMServiceAccessor p_accessor) {
		m_serviceAccessor = p_accessor;
	}

	/**
	 * Set the node id of the node/master this task was submitted to.
	 * @param p_nodeId
	 *            Node id of the node this task was submitted to.
	 */
	void setNodeIdSubmitted(final short p_nodeId) {
		m_nodeIdSubmitted = p_nodeId;
	}

	/**
	 * Trigger callbacks of all listeners: execution has started.
	 */
	void notifyListenersExecutionStarts() {
		for (TaskListener listener : m_completionListeners) {
			listener.taskBeforeExecution(this);
		}
	}

	/**
	 * Trigger callbacks of all listeners: execution finished
	 */
	void notifyListenersExecutionCompleted() {
		for (TaskListener listener : m_completionListeners) {
			listener.taskCompleted(this);
		}
	}
}
