package de.hhu.bsinfo.dxcompute;

import java.util.Vector;

import de.hhu.bsinfo.dxcompute.logger.LOG_LEVEL;
import de.hhu.bsinfo.dxcompute.logger.LoggerDelegate;

public class TaskPipeline 
{
	private String m_name;
	private LoggerDelegate m_loggerDelegate;
	private StorageDelegate m_storageDelegate;
	private TaskDelegate m_taskDelegate;
	private Vector<Task> m_pipeline = new Vector<Task>();
	
	public TaskPipeline(final String p_name)
	{
		m_name = p_name;
	}

	// -------------------------------------------------------------------
	
	public String getName()
	{
		return m_name;
	}
	
	public void pushTask(final Task p_task)
	{
		m_pipeline.add(p_task);
	}
	
	@Override
	public String toString()
	{
		return "TaskPipeline[m_name " + m_name + ", numTasks " + m_pipeline.size() + "]";
	}
	
	// -------------------------------------------------------------------
	
	boolean execute(final Object p_arg)
	{
		Object arg;
		
		arg = p_arg;
		
		log(LOG_LEVEL.LL_INFO, "Executing...");
		
		for (Task task : m_pipeline)
		{
			task.setTaskDelegate(m_taskDelegate);
			task.setStorageDelegate(m_storageDelegate);
			task.setLoggerDelegate(m_loggerDelegate);
			log(LOG_LEVEL.LL_DEBUG, "Executing task " + task);
			arg = task.execute(arg);
			if (task.getExitCode() != 0)
			{
				log(LOG_LEVEL.LL_ERROR, "Executing task " + task + " failed with exit code " 
						+ task.getExitCode() + ", aborting pipeline execution.");
				return false;
			}
			else
				log(LOG_LEVEL.LL_DEBUG, "Executing task " + task + " finished.");
		}
		
		log(LOG_LEVEL.LL_INFO, "Executing done.");
		return true;
	}
	
	void setLoggerDelegate(final LoggerDelegate p_loggerInterface)
	{
		m_loggerDelegate = p_loggerInterface;
	}
	
	void setStorageDelegate(final StorageDelegate p_storageInterface)
	{
		m_storageDelegate = p_storageInterface;
	}
	
	void setTaskDelegate(final TaskDelegate p_taskDelegate)
	{
		m_taskDelegate = p_taskDelegate;
	}
	
	// -------------------------------------------------------------------
	
	private void log(final LOG_LEVEL p_level, final String p_msg)
	{
		if (m_loggerDelegate != null)
			m_loggerDelegate.log(p_level, "TaskPipeline " + m_name, p_msg);
	}
}
