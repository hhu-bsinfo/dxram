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

/**
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 19.10.2016
 */
public class TaskListenerCombined implements TaskListener {

    private TaskListenerBeforExecution m_beforeExecution;
    private TaskListenerTaskCompleted m_taskCompleted;

    public TaskListenerCombined(final TaskListenerBeforExecution p_beforeExecution, final TaskListenerTaskCompleted p_taskCompleted) {

        m_beforeExecution = p_beforeExecution;
        m_taskCompleted = p_taskCompleted;
    }

    @Override
    public void taskBeforeExecution(final Task p_task) {
        m_beforeExecution.taskBeforeExecution(p_task);
    }

    @Override
    public void taskCompleted(final Task p_task) {
        m_taskCompleted.taskCompleted(p_task);
    }
}
