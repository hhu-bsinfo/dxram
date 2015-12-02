package de.uniduesseldorf.dxcompute.job;

import de.uniduesseldorf.dxcompute.logger.LOG_LEVEL;
import de.uniduesseldorf.dxcompute.logger.LoggerDelegate;

public class Worker extends Thread implements JobDelegate
{
	private int m_id;
	private volatile boolean m_running;
	private volatile boolean m_shutdown;
	
	private WorkStealingQueue m_publicLocalQueue = new WorkStealingQueue();
	private WorkStealingQueue m_privateQueue = new WorkStealingQueue();
	
	private WorkerDelegate m_workerDelegate;
	private LoggerDelegate m_loggerDelegate;
	
	public Worker(final int p_id)
	{
		m_id = p_id;
		m_running = false;
		m_shutdown = false;
	}
	
	// -------------------------------------------------------------------
	
	public Job stealJobLocal() 
	{
		return m_publicLocalQueue.steal();
	}
	
	public void shutdown()
	{
		m_shutdown = true;
	}
	
	public boolean isRunning()
	{
		return m_running;
	}
	
	// -------------------------------------------------------------------
	
	@Override
	public void run()
	{
		log(LOG_LEVEL.LL_INFO, "Running...");
		
		m_running = true;
		
		while (true)
		{
			Job job;
			
			job = m_privateQueue.pop();
			if (job != null)
			{
				log(LOG_LEVEL.LL_DEBUG, "Executing job " + job.getJobID() + " from private queue.");
				job.setJobDelegate(this);
				job.setLoggerDelegate(m_loggerDelegate);
				job.execute();
				m_workerDelegate.finishedJob();
				continue;
			}
			
			job = m_publicLocalQueue.pop();
			if (job != null)
			{
				log(LOG_LEVEL.LL_DEBUG, "Executing job " + job.getJobID() + " from public local queue.");
				job.setJobDelegate(this);
				job.setLoggerDelegate(m_loggerDelegate);
				job.execute();
				m_workerDelegate.finishedJob();
				continue;
			}
			
			job = m_workerDelegate.stealJobLocal(this);
			if (job != null)
			{
				log(LOG_LEVEL.LL_DEBUG, "Executing stolen job " + job.getJobID());
				job.setJobDelegate(this);
				job.setLoggerDelegate(m_loggerDelegate);
				job.execute();
				m_workerDelegate.finishedJob();
				continue;
			}
			
			if (m_shutdown)
				break;
			
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		log(LOG_LEVEL.LL_INFO, "Shut down.");
		m_running = false;
	}
	
	@Override
	public boolean pushJobPublicLocalQueue(Job p_job) {
		// TODO implement limit for jobs
		m_workerDelegate.scheduledJob();
		m_publicLocalQueue.push(p_job);
		return true;
	}

	@Override
	public boolean pushJobPrivateQueue(Job p_job) {
		// TODO implement limit for jobs
		m_workerDelegate.scheduledJob();
		m_privateQueue.push(p_job);
		return true;
	}

	@Override
	public int getAssignedWorkerID() {
		return m_id;
	}
	
	// -------------------------------------------------------------------
	
	void setLoggerDelegate(final LoggerDelegate p_loggerDelegate)
	{
		m_loggerDelegate = p_loggerDelegate;
	}
	
	void setWorkerDelegate(final WorkerDelegate p_workerDelegate)
	{
		m_workerDelegate = p_workerDelegate;
	}
	
	// -------------------------------------------------------------------

	private void log(final LOG_LEVEL p_level, final String p_msg) {
		if (m_loggerDelegate != null)
			m_loggerDelegate.log(p_level, "Worker " + m_id, p_msg);
	}
}
