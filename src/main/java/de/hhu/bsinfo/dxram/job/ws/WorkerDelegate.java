/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.job.ws;

import de.hhu.bsinfo.dxram.job.Job;

/**
 * Delegate for worker to provide some information to the Component via callbacks.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public interface WorkerDelegate {
    /**
     * Steal a job from the local queue.
     *
     * @param p_thief
     *         Worker trying to steal.
     * @return Job successfully stolen from another worker or null if no jobs available or stealing failed.
     */
    Job stealJobLocal(final Worker p_thief);

    /**
     * A job was scheduled for execution by a worker.
     *
     * @param p_job
     *         Job which was scheduled.
     */
    void scheduledJob(final Job p_job);

    /**
     * A job is right before getting executed by a worker.
     *
     * @param p_job
     *         Job getting executed.
     */
    void executingJob(final Job p_job);

    /**
     * Execution of a job finished by a worker.
     *
     * @param p_job
     *         Job that finished execution.
     */
    void finishedJob(final Job p_job);
}
