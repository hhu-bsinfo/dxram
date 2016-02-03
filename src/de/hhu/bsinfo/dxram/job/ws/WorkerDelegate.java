package de.hhu.bsinfo.dxram.job.ws;

import de.hhu.bsinfo.dxram.job.Job;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;

/**
 * Delegate for worker to provide some information to the Component via callbacks.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public interface WorkerDelegate 
{
	/**
	 * Steal a job from the local queue.
	 * @param p_thief Worker trying to steal.
	 * @return Job successfully stolen from another worker or null if no jobs available or stealing failed.
	 */
	public Job stealJobLocal(final Worker p_thief);
	
	/**
	 * A job was scheduled for execution by a worker.
	 * @param p_job Job which was scheduled.
	 */
	public void scheduledJob(final Job p_job);
	
	/**
	 * Execution of a job finished by a worker.
	 * @param p_job Job that finished execution.
	 */
	public void finishedJob(final Job p_job);
	
	/**
	 * Get the logger component for logging inside the worker.
	 * @return LoggerComponent.
	 */
	public LoggerComponent getLoggerComponent();
	
	/**
	 * Get the ID of the Node the worker is running on.
	 * @return NodeID.
	 */
	public short getNodeID();
}
