package de.hhu.bsinfo.dxram.job;

import de.hhu.bsinfo.dxram.engine.DXRAMService;

public class Worker extends Thread implements JobDelegate
{
	private int m_id = -1;
	private volatile boolean m_running = false;
	private volatile boolean m_shutdown = false;
	private volatile boolean m_isIdle = false;
	
	private WorkStealingQueue m_queue = new WorkStealingQueue();
	
	private WorkerDelegate m_workerDelegate = null;
	private RemoteSubmissionDelegate m_remoteSubmissionDelegate = null;
	
	public Worker(final int p_id, final WorkerDelegate p_workerDelegate, final RemoteSubmissionDelegate p_remoteSubmissionDelegate)
	{
		m_id = p_id;
		m_workerDelegate = p_workerDelegate;
		m_remoteSubmissionDelegate = p_remoteSubmissionDelegate;
	}
	
	// -------------------------------------------------------------------
	
	public int getID()
	{
		return m_id;
	}
	
	public Job stealJob() 
	{
		return m_queue.steal();
	}
	
	public boolean pushJob(final Job p_job)
	{
		m_workerDelegate.scheduledJob(p_job);
		return m_queue.push(p_job);
	}
	
	public int getQueueJobsScheduled() {
		return m_queue.jobsScheduled();
	}
	
	public boolean isIdle() {
		return m_isIdle;
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
	public String toString()
	{
		return "Worker[m_id " + m_id + ", m_running " + m_running + ", m_shutdown " + m_shutdown + ", m_isIdle " + m_isIdle + "]";
	}
	
	// -------------------------------------------------------------------
	
	@Override
	public void run()
	{
		m_workerDelegate.getLoggerComponent().info(getClass(), "Worker " + m_id + ": Running...");
		
		m_running = true;
		
		while (true)
		{
			Job job;
			
			job = m_queue.pop();
			if (job != null)
			{
				m_isIdle = false;
				m_workerDelegate.getLoggerComponent().debug(getClass(), "Worker " + m_id + ": Executing job " + job + " from queue.");
				job.setJobDelegate(this);
				job.execute(m_workerDelegate.getNodeID());
				m_workerDelegate.finishedJob(job);
				continue;
			}
			
			job = m_workerDelegate.stealJobLocal(this);
			if (job != null)
			{
				m_isIdle = false;
				m_workerDelegate.getLoggerComponent().debug(getClass(), "Worker " + m_id + ": Executing stolen job " + job);
				job.setJobDelegate(this);
				job.execute(m_workerDelegate.getNodeID());
				m_workerDelegate.finishedJob(job);
				continue;
			}
			
			if (m_shutdown)
				break;
			
			m_isIdle = true;
			Thread.yield();
		}
		
		m_workerDelegate.getLoggerComponent().info(getClass(), "Worker " + m_id + ": Shut down.");
		m_running = false;
	}
	
	// -------------------------------------------------------------------
	
	@Override
	public <T extends DXRAMService> T getService(Class<T> p_class) {
		return m_workerDelegate.getService(p_class);
	}
}
