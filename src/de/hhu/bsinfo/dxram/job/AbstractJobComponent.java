package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;

/**
 * Component handling jobs to be executed (local only).
 * 
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public abstract class AbstractJobComponent extends AbstractDXRAMComponent {

	/**
	 * Constructor
	 * @param p_priorityInit Priority for initialization of this component. 
	 * 			When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown Priority for shutting down this component. 
	 * 			When choosing the order, consider component dependencies here.
	 */
	public AbstractJobComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	/**
	 * Schedule a job for execution.
	 * @param p_job Job to schedule for execution.
	 * @return True if scheduling was successful, false otherwise.
	 */
	public abstract boolean pushJob(final Job p_job);
	
	/**
	 * Get the total number of unfinished (local) jobs.
	 * @return Number of unfinished jobs.
	 */
	public abstract long getNumberOfUnfinishedJobs();
	
	/**
	 * Actively wait for all submitted jobs to finish execution.
	 * @return True if waiting was successful and all jobs have finished execution, false otherwise.
	 */
	public abstract boolean waitForSubmittedJobsToFinish();
}
