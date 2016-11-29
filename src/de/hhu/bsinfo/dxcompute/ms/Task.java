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

import java.util.ArrayList;

import de.hhu.bsinfo.ethnet.NodeID;

/**
 * A task that can be submitted to a master-slave compute group.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class Task {

    private TaskPayload m_payload;
    private String m_name;
    private int m_taskIdAssigned = -1;
    private short m_nodeIdSubmitted = -1;
    private int[] m_returnCodes;
    private ArrayList<TaskListener> m_completionListeners = new ArrayList<>();

    /**
     * Constructor
     * @param p_payload
     *            Payload for that task containing the code and data to execute.
     * @param p_name
     *            Name of the task (debug only).
     */
    public Task(final TaskPayload p_payload, final String p_name) {
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
     * Get the task id assigned by the service on submission.
     * @return Task id.
     */
    public int getTaskIdAssigned() {
        return m_taskIdAssigned;
    }

    /**
     * Check if the task has started execution.
     * @return True if started, false otherwise.
     */
    public boolean hasTaskStarted() {
        return m_nodeIdSubmitted != -1;
    }

    /**
     * Get the node id which submitted the task.
     * @return Node id that submitted this task.
     */
    public short getNodeIdSubmitted() {
        return m_nodeIdSubmitted;
    }

    /**
     * Check if the task has completed.
     * @return True if task completed, false otherwise.
     */
    public boolean hasTaskCompleted() {
        return m_returnCodes != null;
    }

    /**
     * Get the return codes after execution finished. If execution hasn't finished, yet,
     * this returns null.
     * @return Return codes after execution has finished.
     */
    public int[] getExecutionReturnCodes() {
        return m_returnCodes;
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
        return "Task[" + m_name + "][" + m_taskIdAssigned + "][" + NodeID.toHexString(m_nodeIdSubmitted) + "]: " + m_payload;
    }

    /**
     * Get the payload coupled to this task.
     * @return TaskPayload
     */
    TaskPayload getPayload() {
        return m_payload;
    }

    /**
     * Assign a task id to this task.
     * @param p_id
     *            Id to assign.
     */
    void assignTaskId(final int p_id) {
        m_taskIdAssigned = p_id;
    }

    /**
     * Set the node id that submitted this task.
     * @param p_id
     *            Node id.
     */
    void setNodeIdSubmitted(final short p_id) {
        m_nodeIdSubmitted = p_id;
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
     * Trigger callbacks of all listeners: execution finished.
     * @param p_returnCodes
     *            Return codes of the slave nodes after execution has finished.
     */
    void notifyListenersExecutionCompleted(final int[] p_returnCodes) {
        m_returnCodes = p_returnCodes;

        for (TaskListener listener : m_completionListeners) {
            listener.taskCompleted(this);
        }
    }
}
