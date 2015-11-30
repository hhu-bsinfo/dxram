package de.uniduesseldorf.dxcompute.job;

import de.uniduesseldorf.dxcompute.logger.LOG_LEVEL;
import de.uniduesseldorf.dxcompute.logger.LoggerInterface;

public class Worker extends Thread implements JobInterface, WorkStealingInterface
{
	private int m_id;
	private volatile boolean m_running;
	private volatile boolean m_shutdown;
	
	private WorkStealingQueue m_publicLocalQueue = new WorkStealingQueue();
	private WorkStealingQueue m_privateQueue = new WorkStealingQueue();
	
	private WorkStealingInterface m_workStealingInterface;
	private LoggerInterface m_loggerInterface;
	
	public Worker(final int p_id)
	{
		m_id = p_id;
		m_running = false;
		m_shutdown = false;
	}
	
	protected void setLoggerInterface(final LoggerInterface p_loggerInterface)
	{
		m_loggerInterface = p_loggerInterface;
	}
	
	protected void setWorkStealingInterface(final WorkStealingInterface p_workStealingInterface)
	{
		m_workStealingInterface = p_workStealingInterface;
	}
	
	public void shutdown()
	{
		m_shutdown = true;
	}
	
	public boolean isRunning()
	{
		return m_running;
	}
	
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
				job.execute(this);
				continue;
			}
			
			job = m_publicLocalQueue.pop();
			if (job != null)
			{
				log(LOG_LEVEL.LL_DEBUG, "Executing job " + job.getJobID() + " from public local queue.");
				job.execute(this);
				continue;
			}
			
			job = m_workStealingInterface.stealJobLocal();
			if (job != null)
			{
				log(LOG_LEVEL.LL_DEBUG, "Executing stolen job " + job.getJobID());
				job.execute(this);
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
		m_publicLocalQueue.push(p_job);
		return true;
	}

	@Override
	public boolean pushJobPrivateQueue(Job p_job) {
		// TODO implement limit for jobs
		m_privateQueue.push(p_job);
		return true;
	}

	@Override
	public int getAssignedWorkerID() {
		return m_id;
	}

	@Override
	public Job stealJobLocal() 
	{
		return m_publicLocalQueue.steal();
	}

	@Override
	public void log(final LOG_LEVEL p_level, final String p_msg) {
		if (m_loggerInterface != null)
			m_loggerInterface.log(p_level, "Worker " + m_id, p_msg);
	}
}
