/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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
 * State holding status information about the encapsulated task
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.01.2017
 */
public final class TaskScriptState {
    private TaskScript m_script;

    private int m_taskScriptIdAssigned = -1;
    private short m_nodeIdSubmitted = -1;
    private int[] m_returnCodes;
    private ArrayList<TaskListener> m_completionListeners = new ArrayList<>();

    /**
     * Constructor
     *
     * @param p_script
     *     Task script to encapsulate
     */
    public TaskScriptState(final TaskScript p_script) {
        m_script = p_script;
    }

    /**
     * Get the task id assigned by the service on submission.
     *
     * @return TaskScript id.
     */
    public int getTaskScriptIdAssigned() {
        return m_taskScriptIdAssigned;
    }

    /**
     * Check if the task script has started execution.
     *
     * @return True if started, false otherwise.
     */
    public boolean hasTaskStarted() {
        return m_nodeIdSubmitted != -1;
    }

    /**
     * Get the node id which submitted the task script.
     *
     * @return Node id that submitted this task.
     */
    public short getNodeIdSubmitted() {
        return m_nodeIdSubmitted;
    }

    /**
     * Check if the task has completed.
     *
     * @return True if task completed, false otherwise.
     */
    public boolean hasTaskCompleted() {
        return m_returnCodes != null;
    }

    /**
     * Get the return codes after execution finished. If execution hasn't finished, yet,
     * this returns null.
     *
     * @return Return codes after execution has finished.
     */
    public int[] getExecutionReturnCodes() {
        return m_returnCodes;
    }

    /**
     * Register a TaskListener for this task.
     *
     * @param p_listeners
     *     Listeners to register.
     */
    public void registerTaskListener(final TaskListener... p_listeners) {
        for (TaskListener listener : p_listeners) {
            m_completionListeners.add(listener);
        }
    }

    @Override
    public String toString() {
        return "TaskScriptState[" + m_taskScriptIdAssigned + "][" + NodeID.toHexString(m_nodeIdSubmitted) + "]: " + m_script;
    }

    /**
     * Get the task script encapsulated by this state.
     *
     * @return TaskScript
     */
    TaskScript getTaskScript() {
        return m_script;
    }

    /**
     * Assign a task id to this task.
     *
     * @param p_id
     *     Id to assign.
     */
    void assignTaskId(final int p_id) {
        m_taskScriptIdAssigned = p_id;
    }

    /**
     * Set the node id that submitted this task.
     *
     * @param p_id
     *     Node id.
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
     *
     * @param p_returnCodes
     *     Return codes of the slave nodes after execution has finished.
     */
    void notifyListenersExecutionCompleted(final int[] p_returnCodes) {
        m_returnCodes = p_returnCodes;

        for (TaskListener listener : m_completionListeners) {
            listener.taskCompleted(this);
        }
    }
}
