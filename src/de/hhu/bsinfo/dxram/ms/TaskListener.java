/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.ms;

/**
 * Listener interface to listen to common task events.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public interface TaskListener {
    /**
     * Gets called when the task is taken from the queue and is about to get executed.
     *
     * @param p_taskScriptState
     *     TaskScriptState of the task to get executed.
     */
    void taskBeforeExecution(final TaskScriptState p_taskScriptState);

    /**
     * Gets called when the task execution has completed.
     *
     * @param p_taskScriptState
     *     TaskScriptState of the task that completed execution.
     */
    void taskCompleted(final TaskScriptState p_taskScriptState);
}
