package de.uniduesseldorf.dxcompute;

import de.uniduesseldorf.dxcompute.logger.LOG_LEVEL;
import de.uniduesseldorf.dxcompute.logger.LoggerDelegate;

public abstract class Task 
{
	private String m_name;
	private TaskDelegate m_taskDelegate;
	private StorageDelegate m_storageDelegate;
	private LoggerDelegate m_loggerDelegate;
	
	public Task(final String p_name)
	{
		m_name = p_name;
	}
	
	// -------------------------------------------------------------------
	
	public String getName()
	{
		return m_name;
	}
	
	@Override
	public String toString()
	{
		return "Task[m_name " + m_name + "]";
	}
	
	// -------------------------------------------------------------------
	
	void setTaskDelegate(final TaskDelegate p_taskDelegate)
	{
		m_taskDelegate = p_taskDelegate;
	}
	
	void setStorageDelegate(final StorageDelegate p_storageDelegate)
	{
		m_storageDelegate = p_storageDelegate;
	}
	
	void setLoggerDelegate(final LoggerDelegate p_loggerDelegate)
	{
		m_loggerDelegate = p_loggerDelegate;
	}
	
	// -------------------------------------------------------------------
	
	protected abstract Object execute(final Object p_arg);
	
	protected TaskDelegate getTaskDelegate()
	{
		return m_taskDelegate;
	}
	
	protected StorageDelegate getStorageDelegate()
	{
		return m_storageDelegate;
	}
	
	protected void log(final LOG_LEVEL p_level, final String p_msg)
	{
		if (m_loggerDelegate != null)
			m_loggerDelegate.log(p_level, "Task " + m_name, p_msg);
	}
}
