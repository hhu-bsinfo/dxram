
package de.hhu.bsinfo.dxcompute.job.ws;

import de.hhu.bsinfo.dxcompute.job.AbstractJob;

/**
 * Worker thread executing jobs using a work stealing approach.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public class Worker extends Thread {
	private int m_id = -1;
	private volatile boolean m_running;
	private volatile boolean m_shutdown;
	private volatile boolean m_isIdle;

	private WorkStealingQueue m_queue = new WorkStealingQueueConcurrentDeque();

	private WorkerDelegate m_workerDelegate;

	/**
	 * Constructor
	 * @param p_id
	 *            ID of this worker.
	 * @param p_workerDelegate
	 *            Delegate for callbacks/notifications.
	 */
	public Worker(final int p_id, final WorkerDelegate p_workerDelegate) {
		super("JobWorker " + p_id);

		m_id = p_id;
		m_workerDelegate = p_workerDelegate;
	}

	// -------------------------------------------------------------------

	/**
	 * Get the ID of this worker.
	 * @return ID of this worker.
	 */
	public int getID() {
		return m_id;
	}

	/**
	 * Steal a job from this worker's queue.
	 * @return Job stolen or null if nothing to steal or stealing failed.
	 */
	public AbstractJob stealJob() {
		return m_queue.steal();
	}

	/**
	 * Push a new job to this worker's queue.
	 * @param p_job
	 *            Job to push to queue.
	 * @return True if pushing job was successful, false if it failed (queue full).
	 */
	public boolean pushJob(final AbstractJob p_job) {
		m_workerDelegate.scheduledJob(p_job);
		return m_queue.push(p_job);
	}

	/**
	 * Get the number of jobs currently scheduled.
	 * @return Num jobs currently scheduled.
	 */
	public int getQueueJobsScheduled() {
		return m_queue.count();
	}

	/**
	 * Check if worker is currently idling.
	 * @return True if idling, false otherwise.
	 */
	public boolean isIdle() {
		return m_isIdle;
	}

	/**
	 * Initiate shutdown of this worker.
	 */
	public void shutdown() {
		m_shutdown = true;
	}

	/**
	 * Check if the worker is running.
	 * @return True if running, false otherwise
	 */
	public boolean isRunning() {
		return m_running;
	}

	@Override
	public String toString() {
		return "Worker[m_id " + m_id + ", m_running " + m_running + ", m_shutdown " + m_shutdown + ", m_isIdle "
				+ m_isIdle + "]";
	}

	// -------------------------------------------------------------------

	@Override
	public void run() {
		// #if LOGGER >= INFO
		m_workerDelegate.getLoggerComponent().info(getClass(), "Worker " + m_id + ": Running...");
		// #endif /* LOGGER >= INFO */

		m_running = true;

		while (true) {
			AbstractJob job;

			job = m_queue.pop();
			if (job != null) {
				m_isIdle = false;

				// #if LOGGER >= DEBUG
				m_workerDelegate.getLoggerComponent().debug(getClass(),
						"Worker " + m_id + ": Executing job " + job + " from queue.");
				// #endif /* LOGGER >= DEBUG */

				m_workerDelegate.executingJob(job);
				job.execute(m_workerDelegate.getNodeID());
				m_workerDelegate.finishedJob(job);
				continue;
			}

			job = m_workerDelegate.stealJobLocal(this);
			if (job != null) {
				m_isIdle = false;

				// #if LOGGER >= DEBUG
				m_workerDelegate.getLoggerComponent().debug(getClass(),
						"Worker " + m_id + ": Executing stolen job " + job);
				// #endif /* LOGGER >= DEBUG */

				m_workerDelegate.executingJob(job);
				job.execute(m_workerDelegate.getNodeID());
				m_workerDelegate.finishedJob(job);
				continue;
			}

			if (m_shutdown) {
				break;
			}

			m_isIdle = true;
			Thread.yield();
		}

		// #if LOGGER >= INFO
		m_workerDelegate.getLoggerComponent().info(getClass(), "Worker " + m_id + ": Shut down.");
		// #endif /* LOGGER >= INFO */

		m_running = false;
	}
}
