package de.hhu.bsinfo.dxram.job;

import java.util.concurrent.atomic.AtomicLong;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.job.ws.Worker;
import de.hhu.bsinfo.dxram.job.ws.WorkerDelegate;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;

/**
 * Implementation of a JobComponent using a work stealing approach for scheduling/load balancing.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public class JobWorkStealingComponent extends JobComponent implements WorkerDelegate {

	private LoggerComponent m_logger = null;
	private BootComponent m_boot = null;
	
	private Worker[] m_workers = null;
	private AtomicLong m_unfinishedJobs = new AtomicLong(0);
	
	/**
	 * Constructor
	 * @param p_priorityInit Priority for initialization of this component. 
	 * 			When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown Priority for shutting down this component. 
	 * 			When choosing the order, consider component dependencies here.
	 */
	public JobWorkStealingComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	@Override
	public boolean pushJob(final Job p_job) {	
		// cause we are using a work stealing approach, we do not need to
		// care about which worker to assign this job to
		
		boolean success = false;
		for (Worker worker : m_workers)
		{
			if (worker.pushJob(p_job))
			{
				// causes the garbage collector to go crazy if too many jobs are pushed very quickly
				//m_logger.debug(getClass(), "Submitted job " + p_job + " to worker " + worker);
				success = true;
				break;
			}
		}
		
		if (!success)
		{
			m_logger.warn(getClass(), "Submiting job " + p_job + " failed.");
		}	
		
		return success;
	}

	@Override
	public long getNumberOfUnfinishedJobs() {
		return m_unfinishedJobs.get();
	}

	@Override
	public boolean waitForSubmittedJobsToFinish() {
		while (m_unfinishedJobs.get() > 0)
		{
			Thread.yield();
		}
		
		return true;
	}

	@Override
	protected void registerDefaultSettingsComponent(Settings p_settings) {
		p_settings.setDefaultValue(JobConfigurationValues.Component.NUM_WORKERS);
	}

	@Override
	protected boolean initComponent(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {
		m_logger = getDependentComponent(LoggerComponent.class);
		m_boot = getDependentComponent(BootComponent.class);
		
		m_workers = new Worker[p_settings.getValue(JobConfigurationValues.Component.NUM_WORKERS)];
		
		for (int i = 0; i < m_workers.length; i++) {
			m_workers[i] = new Worker(i, this);
		}
		
		// avoid race condition by first creating all workers, then starting them
		for (int i = 0; i < m_workers.length; i++) {
			m_workers[i].start();
		}
		
		// wait until all workers are running
		for (int i = 0; i < m_workers.length; i++)
		{
			while (!m_workers[i].isRunning())
			;
		}
		
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		m_logger.debug(getClass(), "Waiting for unfinished jobs...");
		
		while (m_unfinishedJobs.get() > 0)
		{
			Thread.yield();
		}
		
		for (Worker worker : m_workers)
			worker.shutdown();
		
		m_logger.debug(getClass(), "Waiting for workers to shut down...");
		
		for (Worker worker : m_workers)
		{
			while (worker.isRunning())
			{
				Thread.yield();
			}
		}

		return true;
	}

	@Override
	public Job stealJobLocal(Worker p_thief) {
		Job job = null;
		
		for (Worker worker : m_workers)
		{
			// don't steal from own queue
			if (p_thief == worker)
				continue;
			
			job = worker.stealJob();
			if (job != null)
			{
				m_logger.trace(getClass(), "Job " + job + " stolen from worker " + worker);
				break;
			}
		}
		
		return job;
	}

	@Override
	public void scheduledJob(final Job p_job) {
		m_unfinishedJobs.incrementAndGet();
		p_job.notifyListenersJobScheduledForExecution(m_boot.getNodeID());
	}
	
	@Override
	public void executingJob(Job p_job) {
		p_job.notifyListenersJobStartsExecution(m_boot.getNodeID());
	}

	@Override
	public void finishedJob(final Job p_job) {
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
