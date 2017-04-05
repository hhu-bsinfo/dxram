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

package de.hhu.bsinfo.dxram.ms;

/**
 * Base class for all tasks to be implemented. This holds optional data the task
 * needs for execution as well as the code getting executed for the task.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public interface Task extends TaskScriptNode {

    /**
     * Execute function to implement with the task/code to execute.
     *
     * @param p_ctx
     *     Context for this task containing DXRAM access and task parameters.
     * @return Return code of your task. 0 on success, everything else indicates an error.
     */
    int execute(final TaskContext p_ctx);

    /**
     * Handle a signal from the master
     *
     * @param p_signal
     *     Signal from the master
     */
    void handleSignal(final Signal p_signal);
}
