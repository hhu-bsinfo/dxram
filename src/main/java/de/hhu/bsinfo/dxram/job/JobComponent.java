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

import java.util.concurrent.atomic.AtomicLong;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMModule;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.engine.ModuleAccessor;
import de.hhu.bsinfo.dxram.job.ws.Worker;
import de.hhu.bsinfo.dxram.job.ws.WorkerDelegate;

/**
 * Implementation of a JobComponent using a work stealing approach for scheduling/load balancing.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
@AbstractDXRAMModule.Attributes(supportsSuperpeer = false, supportsPeer = true)
@AbstractDXRAMComponent.Attributes(priorityInit = DXRAMComponentOrder.Init.JOB,
        priorityShutdown = DXRAMComponentOrder.Shutdown.JOB)
public class JobComponent extends AbstractDXRAMComponent<JobComponentConfig>
        implements WorkerDelegate {
    // component dependencies
    private AbstractBootComponent m_boot;

    private boolean m_enabled;
    private Worker[] m_workers;
    private AtomicLong m_unfinishedJobs = new AtomicLong(0);

    public boolean pushJob(final AbstractJob p_job) {
        if (!m_enabled) {
            LOGGER.warn("Cannot push job, disabled");
            return false;
        }

        // gives the job access to dxram services
        p_job.setServiceAccessor(getParentEngine());

        // cause we are using a work stealing approach, we do not need to
        // care about which worker to assign this job to

        boolean success = false;

        for (Worker worker : m_workers) {
            if (worker.pushJob(p_job)) {
                // causes the garbage collector to go crazy if too many jobs are pushed very quickly
                LOGGER.debug("Submitted job %s to worker %s", p_job, worker);

                success = true;
                break;
            }
        }

        if (!success) {
            LOGGER.warn("Submiting job %s failed", p_job);
        }

        return success;
    }

    public long getNumberOfUnfinishedJobs() {
        if (!m_enabled) {
            return 0;
        }

        return m_unfinishedJobs.get();
    }

    public boolean waitForSubmittedJobsToFinish() {
        if (!m_enabled) {
            return true;
        }

        while (m_unfinishedJobs.get() > 0) {
            Thread.yield();
        }

        return true;
    }

    @Override
    protected void resolveComponentDependencies(final ModuleAccessor p_moduleAccessor) {
        m_boot = p_moduleAccessor.getComponent(AbstractBootComponent.class);
    }

    @Override
    protected boolean initComponent(final DXRAMConfig p_config, final DXRAMJNIManager p_jniManager) {
        JobComponentConfig config = p_config.getComponentConfig(JobComponent.class);

        m_enabled = config.isEnabled();

        if (m_enabled) {
            LOGGER.info("JobWorkStealing enabled");

            m_workers = new Worker[getConfig().getNumWorkers()];

            for (int i = 0; i < m_workers.length; i++) {
                m_workers[i] = new Worker(i, this);
            }

            // avoid race condition by first creating all workers, then starting them
            for (Worker worker : m_workers) {
                worker.start();
            }

            // wait until all workers are running
            for (Worker worker : m_workers) {
                while (!worker.isRunning()) {
                    Thread.yield();
                }
            }
        }

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        if (m_enabled) {
            LOGGER.debug("Waiting for unfinished jobs...");

            while (m_unfinishedJobs.get() > 0) {
                Thread.yield();
            }

            for (Worker worker : m_workers) {
                worker.shutdown();
            }

            LOGGER.debug("Waiting for workers to shut down...");

            for (Worker worker : m_workers) {
                while (worker.isRunning()) {
                    Thread.yield();
                }
            }
        }

        return true;
    }

    @Override
    public AbstractJob stealJobLocal(final Worker p_thief) {
        AbstractJob job = null;

        for (Worker worker : m_workers) {
            // don't steal from own queue
            if (p_thief == worker) {
                continue;
            }

            job = worker.stealJob();

            if (job != null) {
                LOGGER.trace("Job %s stolen from worker %s", job, worker);

                break;
            }
        }

        return job;
    }

    @Override
    public void scheduledJob(final AbstractJob p_job) {
        m_unfinishedJobs.incrementAndGet();
        p_job.notifyListenersJobScheduledForExecution(m_boot.getNodeId());
    }

    @Override
    public void executingJob(final AbstractJob p_job) {
        p_job.notifyListenersJobStartsExecution(m_boot.getNodeId());
    }

    @Override
    public void finishedJob(final AbstractJob p_job) {
        m_unfinishedJobs.decrementAndGet();
        p_job.notifyListenersJobFinishedExecution(m_boot.getNodeId());
    }
}
