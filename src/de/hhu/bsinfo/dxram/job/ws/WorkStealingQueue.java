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

package de.hhu.bsinfo.dxram.job.ws;

import de.hhu.bsinfo.dxram.job.AbstractJob;

/**
 * Interface for a work stealing queue.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public interface WorkStealingQueue {
    /**
     * Get the number of jobs currently in the queue.
     *
     * @return Number of jobs in queue (might not be total queue size).
     */
    int count();

    /**
     * Push a job to the back of the queue.
     *
     * @param p_job
     *         Job to add to the queue.
     * @return True if adding was successful, false if failed (queue full).
     */
    boolean push(final AbstractJob p_job);

    /**
     * Pop a job from the back of the queue.
     *
     * @return Job from the back of the queue or null if queue empty.
     */
    AbstractJob pop();

    /**
     * Steal a job from the front of the queue.
     *
     * @return Job from the front of the queue or null if stealing failed or queue empty.
     */
    AbstractJob steal();
}
