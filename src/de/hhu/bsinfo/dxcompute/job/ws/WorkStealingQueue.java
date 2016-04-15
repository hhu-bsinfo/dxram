
package de.hhu.bsinfo.dxcompute.job.ws;

import de.hhu.bsinfo.dxcompute.job.AbstractJob;

/**
 * Interface for a work stealing queue.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public interface WorkStealingQueue {
	/**
	 * Get the number of jobs currently in the queue.
	 * @return Number of jobs in queue (might not be total queue size).
	 */
	int count();

	/**
	 * Push a job to the back of the queue.
	 * @param p_job
	 *            Job to add to the queue.
	 * @return True if adding was successful, false if failed (queue full).
	 */
	boolean push(final AbstractJob p_job);

	/**
	 * Pop a job from the back of the queue.
	 * @return Job from the back of the queue or null if queue empty.
	 */
	AbstractJob pop();

	/**
	 * Steal a job from the front of the queue.
	 * @return Job from the front of the queue or null if stealing failed or queue empty.
	 */
	AbstractJob steal();
}
