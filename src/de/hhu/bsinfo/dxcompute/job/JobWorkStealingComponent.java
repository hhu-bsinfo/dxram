package de.hhu.bsinfo.dxcompute.job;

import java.util.concurrent.atomic.AtomicLong;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;

public class JobWorkStealingComponent extends JobComponent implements WorkerDelegate {

	private LoggerComponent m_logger = null;
	private BootComponent m_boot = null;
	
	private Worker[] m_workers = null;
	private AtomicLong m_unfinishedJobs = new AtomicLong(0);
	
	private RemoteSubmissionDelegate m_remoteSubmissionDelegate = null;
	
	public JobWorkStealingComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}
	
	@Override
	public void setRemoteSubsmissionDelegate(final RemoteSubmissionDelegate p_delegate)
	{
		m_remoteSubmissionDelegate = p_delegate;
	}

	@Override
	public boolean submit(final Job p_job) {	
		// cause we are using a work stealing approach, we do not need to
		// care about which worker to assign this job to
		
		boolean success = false;
		for (Worker worker : m_workers)
		{
			if (worker.pushJobPublicLocalQueue(p_job))
			{
				m_logger.info(getClass(), "Submited job " + p_job.getID() + " to worker " + worker.getAssignedWorkerID());
				success = true;
				break;
			}
		}
		
		if (!success)
			m_logger.warn(getClass(), "Submiting job " + p_job.getID() + " failed.");
			
		return success;
	}

	@Override
	public long getNumberOfUnfinishedJobs() {
		return m_unfinishedJobs.get();
	}

	@Override
	public void waitForSubmittedJobsToFinish() {
		while (m_unfinishedJobs.get() > 0)
		{
			Thread.yield();
		}
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
		
		for (int i = 0; i < m_workers.length; i++)
		{
			m_workers[i] = new Worker(i, this, m_remoteSubmissionDelegate);
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
		
		// TODO have better pattern for stealing
		for (Worker worker : m_workers)
		{
			// don't steal from own queue
			if (p_thief == worker)
				continue;
			
			job = worker.stealJobLocal();
			if (job != null)
			{
				m_logger.debug(getClass(), "Job " + job.getID() + " stolen from worker " + worker.getAssignedWorkerID());
				break;
			}
		}
		
		return job;
	}

	@Override
	public void scheduledJob() {
		m_unfinishedJobs.incrementAndGet();
	}

	@Override
	public void finishedJob() {
		m_unfinishedJobs.decrementAndGet();
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
