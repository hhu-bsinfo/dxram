package de.hhu.bsinfo.dxram.job.ws;

import de.hhu.bsinfo.dxram.job.Job;

/**
 * Interface for a work stealing queue.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public interface WorkStealingQueue {
	/**
	 * Get the number of jobs currently in the queue.
	 * @return Number of jobs in queue (might not be total queue size).
	 */
	public int count();
	
	/**
	 * Push a job to the back of the queue.
	 * @param job Job to add to the queue.
	 * @return True if adding was successful, false if failed (queue full).
	 */
	public boolean push(final Job job);
	
	/**
	 * Pop a job from the back of the queue.
	 * @return Job from the back of the queue or null if queue empty.
	 */
	public Job pop();
	
	/**
	 * Steal a job from the front of the queue.
	 * @return Job from the front of the queue or null if stealing failed or queue empty.
	 */
	public Job steal();
}
