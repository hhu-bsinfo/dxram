package de.hhu.bsinfo.dxcompute.job;

public class Worker extends Thread implements JobDelegate
{
	private int m_id = -1;
	private volatile boolean m_running = false;
	private volatile boolean m_shutdown = false;
	private volatile boolean m_isIdle = false;
	
	private WorkStealingQueue m_publicLocalQueue = new WorkStealingQueue();
	private WorkStealingQueue m_privateQueue = new WorkStealingQueue();
	
	private WorkerDelegate m_workerDelegate = null;
	private RemoteSubmissionDelegate m_remoteSubmissionDelegate = null;
	
	public Worker(final int p_id, final WorkerDelegate p_workerDelegate, final RemoteSubmissionDelegate p_remoteSubmissionDelegate)
	{
		m_id = p_id;
		m_workerDelegate = p_workerDelegate;
		m_remoteSubmissionDelegate = p_remoteSubmissionDelegate;
	}
	
	// -------------------------------------------------------------------
	
	public Job stealJobLocal() 
	{
		return m_publicLocalQueue.steal();
	}
	
	public int getPublicLocalQueueJobsScheduled() {
		return m_publicLocalQueue.jobsScheduled();
	}

	public int getPrivateQueueJobsScheduled() {
		return m_privateQueue.jobsScheduled();
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
	
	// -------------------------------------------------------------------
	
	@Override
	public void run()
	{
		m_workerDelegate.getLoggerComponent().info(getClass(), "Worker " + m_id + ": Running...");
		
		m_running = true;
		
		while (true)
		{
			Job job;
			
			job = m_privateQueue.pop();
			if (job != null)
			{
				m_isIdle = false;
				m_workerDelegate.getLoggerComponent().debug(getClass(), "Worker " + m_id + ": Executing job " + job.getID() + " from private queue.");
				job.setJobDelegate(this);
				job.execute(m_workerDelegate.getNodeID());
				m_workerDelegate.finishedJob();
				continue;
			}
			
			job = m_publicLocalQueue.pop();
			if (job != null)
			{
				m_isIdle = false;
				m_workerDelegate.getLoggerComponent().debug(getClass(), "Worker " + m_id + ": Executing job " + job.getID() + " from public local queue.");
				job.setJobDelegate(this);
				job.execute(m_workerDelegate.getNodeID());
				m_workerDelegate.finishedJob();
				continue;
			}
			
			job = m_workerDelegate.stealJobLocal(this);
			if (job != null)
			{
				m_isIdle = false;
				m_workerDelegate.getLoggerComponent().debug(getClass(), "Worker " + m_id + ": Executing stolen job " + job.getID());
				job.setJobDelegate(this);
				job.execute(m_workerDelegate.getNodeID());
				m_workerDelegate.finishedJob();
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
	
	@Override
	public boolean pushJobPublicRemoteQueue(Job p_job, short p_nodeID) 
	{
		return m_remoteSubmissionDelegate.pushJobRemoteQueue(p_job, p_nodeID);
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

	@Override
	public void log(Job p_job, String p_msg) {
		m_workerDelegate.getLoggerComponent().debug(Job.class, "Job-" + p_job.getID() + ": " + p_msg);
	}
}
