package de.hhu.bsinfo.dxcompute.job;

import de.hhu.bsinfo.dxcompute.logger.LOG_LEVEL;
import de.hhu.bsinfo.dxcompute.logger.LoggerDelegate;

public abstract class Job 
{
	private JobDelegate m_jobDelegate;
	private LoggerDelegate m_loggerDelegate;
	
	public Job()
	{
		
	}
	
	// -------------------------------------------------------------------
	
	public abstract long getJobID();
	
	// -------------------------------------------------------------------
	
	void setJobDelegate(final JobDelegate p_jobDelegate)
	{
		m_jobDelegate = p_jobDelegate;
	}
	
	void setLoggerDelegate(final LoggerDelegate p_loggerDelegate)
	{
		m_loggerDelegate = p_loggerDelegate;
	}
	
	// -------------------------------------------------------------------
	
	protected abstract void execute(); 
	
	protected JobDelegate getJobInterface()
	{
		return m_jobDelegate;
	}

	protected void log(final LOG_LEVEL p_level, final String p_msg)
	{
		if (m_loggerDelegate != null)
			m_loggerDelegate.log(p_level, "Job " + getJobID(), p_msg);
	}
}