package de.hhu.bsinfo.dxgraph;

import java.util.Vector;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.logger.LoggerService;

// base class for pipelines
// consisting of multiple tasks
public abstract class GraphTaskPipeline 
{
	private DXRAM m_dxram = null;
	protected LoggerService m_loggerService = null;
	protected BootService m_bootService = null;
	
	private boolean m_recordTaskStatistics = false;
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
			m_loggerService.info(getClass(), "Executing task " + task + "...");
			task.setDXRAM(m_dxram);
			if (!task.executeTask(m_recordTaskStatistics))
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
		m_bootService = p_dxram.getService(BootService.class);
	}
	
	protected void recordTaskStatistics(final boolean p_record) {
		m_recordTaskStatistics = p_record;
	}
	
	protected void pushTask(final GraphTask p_task)
	{
		m_tasks.add(p_task);
	}
}
