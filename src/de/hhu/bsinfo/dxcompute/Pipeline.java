package de.hhu.bsinfo.dxcompute;

import java.util.Vector;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Very often, there is more than one task forming the whole computation.
 * A pipeline consists of multiple tasks, which are executed in order
 * with synchronization between tasks or some of them running in parallel.
 * The pipeline is the "main program" being executed by DXCompute.
 * 
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 *
 */
public abstract class Pipeline 
{
	private DXRAM m_dxram = null;
	protected LoggerService m_loggerService = null;
	protected BootService m_bootService = null;
	
	private boolean m_recordTaskStatistics = false;
	private Vector<Task> m_tasks = new Vector<Task>();
	
	protected ArgumentList m_arguments = new ArgumentList();
	
	/**
	 * Constructor without arguments.
	 */
	public Pipeline()
	{
		
	}
	
	/**
	 * Constructor
	 * @param p_arguments Arguments for the pipeline.
	 */
	public Pipeline(final ArgumentList p_arguments)
	{
		m_arguments = p_arguments;
	}
	
	/**
	 * Execute this pipeline in the current thread.
	 * @return True if execution was successful, false otherwise.
	 */
	public boolean execute()
	{
		m_loggerService.info(getClass(), "Setup pipeline with " + m_arguments.size() + " arguments...");
		if (!setup(m_arguments))
		{
			m_loggerService.error(getClass(), "Setup of pipeline failed.");
			return false;
		}
		
		m_loggerService.info(getClass(), "Executing pipeline with " + m_tasks.size() + " tasks...");
		for (Task task : m_tasks)
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
	
	/**
	 * Function used by DXCompute to setup the pipeline.
	 * @param p_dxram DXRAM instance to use.
	 */
	void setDXRAM(final DXRAM p_dxram)
	{
		m_dxram = p_dxram;
		m_loggerService = p_dxram.getService(LoggerService.class);
		m_bootService = p_dxram.getService(BootService.class);
	}
	
	/**
	 * Implement this function to setup the pipeline by
	 * adding tasks and setting up parameters.
	 * @param p_arguments Arguments for the pipeline.
	 * @return True if setup was successful, false otherwise.
	 */
	protected abstract boolean setup(final ArgumentList p_arguments);
	
	/**
	 * Enable/Disable recording of statistics for every task.
	 * @param p_record True to enable recording, false to disable.
	 */
	protected void recordTaskStatistics(final boolean p_record) {
		m_recordTaskStatistics = p_record;
	}
	
	/**
	 * Call this in setup() to add a task to the pipeline. 
	 * Make sure to push them in the correct order (FIFO).
	 * @param p_task Task to add to the pipeline.
	 */
	protected void pushTask(final Task p_task)
	{
		m_tasks.add(p_task);
	}
}
