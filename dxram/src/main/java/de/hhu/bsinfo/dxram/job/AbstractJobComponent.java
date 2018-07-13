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

package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;

/**
 * Component handling jobs to be executed (local only).
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public abstract class AbstractJobComponent<T extends AbstractDXRAMComponentConfig> extends AbstractDXRAMComponent<T> {
    /**
     * Constructor
     *
     * @param p_priorityInit
     *         Default init priority for this component
     * @param p_priorityShutdown
     *         Default shutdown priority for this component
     */
    public AbstractJobComponent(final short p_priorityInit, final short p_priorityShutdown,
            final Class<T> p_configClass) {
        super(p_priorityInit, p_priorityShutdown, p_configClass);
    }

    /**
     * Schedule a job for execution.
     *
     * @param p_job
     *         Job to schedule for execution.
     * @return True if scheduling was successful, false otherwise.
     */
    public abstract boolean pushJob(final AbstractJob p_job);

    /**
     * Get the total number of unfinished (local) jobs.
     *
     * @return Number of unfinished jobs.
     */
    public abstract long getNumberOfUnfinishedJobs();

    /**
     * Actively wait for all submitted jobs to finish execution.
     *
     * @return True if waiting was successful and all jobs have finished execution, false otherwise.
     */
    public abstract boolean waitForSubmittedJobsToFinish();
}
