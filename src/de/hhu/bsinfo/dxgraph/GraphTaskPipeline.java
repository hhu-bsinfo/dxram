package de.hhu.bsinfo.dxgraph;

import java.util.Vector;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.logger.LoggerService;

// TODO base class for pipelines
// consisting of multiple tasks
public abstract class GraphTaskPipeline 
{
	private DXRAM m_dxram = null;
	private LoggerService m_loggerService = null;
	
	private Vector<GraphTask> m_tasks = new Vector<GraphTask>();
	
	public GraphTaskPipeline()
	{
		
	}
	
	public abstract boolean setup();
	
	public boolean execute()
	{
		m_loggerService.info(getClass(), "Executing pipeline with " + m_tasks.size() + " tasks...");
		for (GraphTask task : m_tasks)
		{
			m_loggerService.debug(getClass(), "Executing task " + task + "...");
			task.setDXRAM(m_dxram);
			if (!task.execute())
			{
				m_loggerService.error(getClass(), "Executing task " + task + " failed, aborting pipeline execution.");
				return false;
			}
		}
		m_loggerService.info(getClass(), "Executing pipeline successful.");
		
		return true;
	}
	
	void setDXRAM(final DXRAM p_dxram)
	{
		m_dxram = p_dxram;
		m_loggerService = p_dxram.getService(LoggerService.class);
	}
	
	protected void log(final String p_msg)
	{
		m_loggerService.debug(getClass(), p_msg);
	}
	
	protected void logError(final String p_msg)
	{
		m_loggerService.error(getClass(), p_msg);
	}
	
	protected void pushTask(final GraphTask p_task)
	{
		m_tasks.add(p_task);
	}
}
