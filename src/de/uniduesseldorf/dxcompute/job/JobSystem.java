package de.uniduesseldorf.dxcompute.job;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import de.uniduesseldorf.dxcompute.logger.LOG_LEVEL;
import de.uniduesseldorf.dxcompute.logger.LoggerDelegate;

public class JobSystem implements WorkerDelegate
{
	private String m_name;
	private LoggerDelegate m_loggerDelegate;

	private Worker[] m_workers;
	private AtomicLong m_unfinishedJobs = new AtomicLong(0);
	
	public JobSystem(final String p_name, final LoggerDelegate p_loggerDelegate)
	{
		m_name = p_name;
		m_loggerDelegate = p_loggerDelegate;
	}
	
	// -------------------------------------------------------------------
	
	public void init(final int p_numThreads)
	{
		log(LOG_LEVEL.LL_INFO, "Initializing...");
		
		m_workers = new Worker[p_numThreads];
		
		for (int i = 0; i < p_numThreads; i++)
		{
			m_workers[i] = new Worker(i);
			m_workers[i].setWorkerDelegate(this);
			m_workers[i].setLoggerDelegate(m_loggerDelegate);
			m_workers[i].start();
		}
		
		// wait until all workers are running
		for (int i = 0; i < p_numThreads; i++)
		{
			while (!m_workers[i].isRunning())
			;
		}
		
		log(LOG_LEVEL.LL_INFO, "Initializing done.");
	}
	
	public long getNumberOfUnfinishedJobs()
	{
		return m_unfinishedJobs.get();
	}
	
	public boolean submit(final Job p_job)
	{
		// choose random worker
		// TODO choose with least load?
		Random ran = new Random();
		Worker worker = m_workers[ran.nextInt(m_workers.length)];
		worker.pushJobPublicLocalQueue(p_job);
		
		log(LOG_LEVEL.LL_INFO, "Submited job " + p_job.getJobID() + " to worker " + worker.getAssignedWorkerID());
		
		return true;
	}
	
	public void waitForSubmittedJobsToFinish()
	{
		while (m_unfinishedJobs.get() > 0)
		{
			Thread.yield();
		}
	}
	
	public void shutdown()
	{
		log(LOG_LEVEL.LL_INFO, "Shutting down...");
		
		log(LOG_LEVEL.LL_DEBUG, "Waiting for unfinished jobs...");
		
		while (m_unfinishedJobs.get() > 0)
		{
			Thread.yield();
		}
		
		for (Worker worker : m_workers)
			worker.shutdown();
		
		log(LOG_LEVEL.LL_DEBUG, "Waiting for workers to shut down...");
		
		for (Worker worker : m_workers)
		{
			while (worker.isRunning())
			{
				Thread.yield();
			}
		}
		
		log(LOG_LEVEL.LL_INFO, "Shutting down done.");
	}
	
	// -------------------------------------------------------------------

	@Override
	public Job stealJobLocal(final Worker p_thief) 
	{
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
				log(LOG_LEVEL.LL_DEBUG, "Job " + job.getJobID() + " stolen from worker " + worker.getAssignedWorkerID());
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
	
	// -------------------------------------------------------------------
	
	private void log(final LOG_LEVEL p_level, final String p_msg)
	{
		if (m_loggerDelegate != null)
			m_loggerDelegate.log(p_level, "JobSystem " + m_name, p_msg);
	}
}
