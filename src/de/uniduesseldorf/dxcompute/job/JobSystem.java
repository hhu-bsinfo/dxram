package de.uniduesseldorf.dxcompute.job;

import java.util.Random;

import de.uniduesseldorf.dxcompute.logger.LOG_LEVEL;
import de.uniduesseldorf.dxcompute.logger.LoggerInterface;

public class JobSystem implements WorkStealingInterface
{
	private String m_name;
	private LoggerInterface m_loggerInterface;

	private Worker[] m_workers;
	
	public JobSystem(final String p_name, final LoggerInterface p_logger)
	{
		m_name = p_name;
		m_loggerInterface = p_logger;
	}
	
	public void init(final int p_numThreads)
	{
		log(LOG_LEVEL.LL_INFO, "Initializing...");
		
		m_workers = new Worker[p_numThreads];
		
		for (int i = 0; i < p_numThreads; i++)
		{
			m_workers[i] = new Worker(i);
			m_workers[i].setWorkStealingInterface(this);
			m_workers[i].setLoggerInterface(m_loggerInterface);
			m_workers[i].start();
		}
		
		log(LOG_LEVEL.LL_INFO, "Initializing done.");
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
	
	public void shutdown()
	{
		log(LOG_LEVEL.LL_INFO, "Shutting down...");
		
		for (Worker worker : m_workers)
		{
			worker.shutdown();
		}
		for (Worker worker : m_workers)
		{
			while (!worker.isRunning())
			;
		}
		
		log(LOG_LEVEL.LL_INFO, "Shutting down done.");
	}

	@Override
	public Job stealJobLocal() 
	{
		Job job = null;
		
		// TODO have better pattern for stealing
		for (Worker worker : m_workers)
		{
			job = worker.stealJobLocal();
			if (job != null)
			{
				log(LOG_LEVEL.LL_DEBUG, "Job " + job.getJobID() + " stolen from worker " + worker.getAssignedWorkerID());
				break;
			}
		}
		
		return job;
	}
	
	private void log(final LOG_LEVEL p_level, final String p_msg)
	{
		if (m_loggerInterface != null)
			m_loggerInterface.log(p_level, "JobSystem " + m_name, p_msg);
	}
}
