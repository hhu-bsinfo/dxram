package de.uniduesseldorf.dxcompute;

import de.uniduesseldorf.dxcompute.job.JobSystem;
import de.uniduesseldorf.dxcompute.logger.LOG_LEVEL;
import de.uniduesseldorf.dxcompute.logger.LoggerDelegate;

public class DXCompute implements TaskDelegate
{
	private LoggerDelegate m_loggerDelegate;
	private StorageDelegate m_storageDelegate;
	
	private JobSystem m_jobSystem;
	
	public DXCompute(final LoggerDelegate p_loggerDelegate)
	{
		m_loggerDelegate = p_loggerDelegate;
		m_jobSystem = new JobSystem("DXCompute", p_loggerDelegate);
	}
	
	// -------------------------------------------------------------------------
	
	public boolean init(final int p_numThreads, final StorageDelegate p_storageDelegate)
	{
		log(LOG_LEVEL.LL_INFO, "Initializing...");
		
		m_storageDelegate = p_storageDelegate;
	
		m_jobSystem.init(p_numThreads);
		
		log(LOG_LEVEL.LL_INFO, "Initializing done.");
		
		return true;
	}
	
	public void execute(final TaskPipeline p_taskPipeline, final Object p_arg)
	{
		log(LOG_LEVEL.LL_INFO, "Executing pipeline " + p_taskPipeline);
		
		p_taskPipeline.setLoggerDelegate(m_loggerDelegate);
		p_taskPipeline.setStorageDelegate(m_storageDelegate);
		p_taskPipeline.setTaskDelegate(this);
		p_taskPipeline.execute(p_arg);
	}
	
	public void shutdown()
	{
		log(LOG_LEVEL.LL_INFO, "Shutting down...");
		
		m_jobSystem.shutdown();
		
		log(LOG_LEVEL.LL_INFO, "Shutting down done.");
	}
	
	// -------------------------------------------------------------------------
	
	@Override
	public void submitJob(ComputeJob p_job) {
		m_jobSystem.submit(p_job);
	}
	
	@Override
	public void waitForSubmittedJobsToFinish() {
		m_jobSystem.waitForSubmittedJobsToFinish();	
	}
	
	// -------------------------------------------------------------------------

	private void log(LOG_LEVEL p_level, String p_msg) {
		if (m_loggerDelegate != null)
			m_loggerDelegate.log(p_level, "DXCompute", p_msg);
	}

}
