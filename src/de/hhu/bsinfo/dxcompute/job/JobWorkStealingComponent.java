
package de.hhu.bsinfo.dxcompute.job;

import java.util.concurrent.atomic.AtomicLong;

import de.hhu.bsinfo.dxcompute.job.ws.Worker;
import de.hhu.bsinfo.dxcompute.job.ws.WorkerDelegate;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;

/**
 * Implementation of a JobComponent using a work stealing approach for scheduling/load balancing.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public class JobWorkStealingComponent extends AbstractJobComponent implements WorkerDelegate {

	private LoggerComponent m_logger;
	private AbstractBootComponent m_boot;

	private Worker[] m_workers;
	private AtomicLong m_unfinishedJobs = new AtomicLong(0);

	/**
	 * Constructor
	 *
	 * @param p_priorityInit     Priority for initialization of this component.
	 *                           When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown Priority for shutting down this component.
	 *                           When choosing the order, consider component dependencies here.
	 */
	public JobWorkStealingComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	@Override
	public boolean pushJob(final AbstractJob p_job) {
		// cause we are using a work stealing approach, we do not need to
		// care about which worker to assign this job to

		boolean success = false;
		for (Worker worker : m_workers) {
			if (worker.pushJob(p_job)) {
				// causes the garbage collector to go crazy if too many jobs are pushed very quickly
				// #if LOGGER >= DEBUG
				m_logger.debug(getClass(), "Submitted job " + p_job + " to worker " + worker);
				// #endif /* LOGGER >= DEBUG */

				success = true;
				break;
			}
		}

		// #if LOGGER >= WARN
		if (!success) {
			m_logger.warn(getClass(), "Submiting job " + p_job + " failed.");
		}
		// #endif /* LOGGER >= WARN */

		return success;
	}

	@Override
	public long getNumberOfUnfinishedJobs() {
		return m_unfinishedJobs.get();
	}

	@Override
	public boolean waitForSubmittedJobsToFinish() {
		while (m_unfinishedJobs.get() > 0) {
			Thread.yield();
		}

		return true;
	}

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {
		p_settings.setDefaultValue(JobConfigurationValues.Component.NUM_WORKERS);
	}

	@Override
	protected boolean initComponent(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		m_logger = getDependentComponent(LoggerComponent.class);
		m_boot = getDependentComponent(AbstractBootComponent.class);

		m_workers = new Worker[p_settings.getValue(JobConfigurationValues.Component.NUM_WORKERS)];

		for (int i = 0; i < m_workers.length; i++) {
			m_workers[i] = new Worker(i, this);
		}

		// avoid race condition by first creating all workers, then starting them
		for (int i = 0; i < m_workers.length; i++) {
			m_workers[i].start();
		}

		// wait until all workers are running
		for (int i = 0; i < m_workers.length; i++) {
			while (!m_workers[i].isRunning()) {
			}
		}

		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		// #if LOGGER >= DEBUG
		m_logger.debug(getClass(), "Waiting for unfinished jobs...");
		// #endif /* LOGGER >= DEBUG */

		while (m_unfinishedJobs.get() > 0) {
			Thread.yield();
		}

		for (Worker worker : m_workers) {
			worker.shutdown();
		}

		// #if LOGGER >= DEBUG
		m_logger.debug(getClass(), "Waiting for workers to shut down...");
		// #endif /* LOGGER >= DEBUG */

		for (Worker worker : m_workers) {
			while (worker.isRunning()) {
				Thread.yield();
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
				// #if LOGGER == TRACE
				m_logger.trace(getClass(), "Job " + job + " stolen from worker " + worker);
				// #endif /* LOGGER == TRACE */
				break;
			}
		}

		return job;
	}

	@Override
	public void scheduledJob(final AbstractJob p_job) {
		m_unfinishedJobs.incrementAndGet();
		p_job.notifyListenersJobScheduledForExecution(m_boot.getNodeID());
	}

	@Override
	public void executingJob(final AbstractJob p_job) {
		p_job.notifyListenersJobStartsExecution(m_boot.getNodeID());
	}

	@Override
	public void finishedJob(final AbstractJob p_job) {
		m_unfinishedJobs.decrementAndGet();
		p_job.notifyListenersJobFinishedExecution(m_boot.getNodeID());
	}

	@Override
	public LoggerComponent getLoggerComponent() {
		return m_logger;
	}

	@Override
	public short getNodeID() {
		return m_boot.getNodeID();
	}
}
