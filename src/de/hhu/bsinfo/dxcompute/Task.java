package de.hhu.bsinfo.dxcompute;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorder;
import de.hhu.bsinfo.dxram.stats.StatisticsService;

/**
 * Base class for a computing task, which is part of a pipeline.
 * Split your work into several modular tasks which can be
 * reused with different pipelines. For example loading, data generation,
 * actual computation/algorithms, verification...
 * 
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 *
 */
public abstract class Task 
{
	private DXRAM m_dxram = null;
	protected LoggerService m_loggerService = null;
	protected BootService m_bootService = null;
	protected ChunkService m_chunkService = null;
	protected JobService m_jobService = null;
	protected NetworkService m_networkService = null;
	protected StatisticsService m_statisticsService = null;
	
	private TaskStatisticsRecorderIDs m_statisticsRecorderIDs = null;
	
	/**
	 * Constructor
	 */
	public Task()
	{
		
	}
	
	/**
	 * Print all recorded statistics of this task to the console.
	 */
	public void printStatistics() {
		m_statisticsService.printStatistics(getClass());
	}
	
	/**
	 * Get all recorded statistics of this task.
	 * @return StatisticsRecorder containing all recorded statistics of this task.
	 */
	public StatisticsRecorder getStatistics() {
		return m_statisticsService.getRecorder(getClass());
	}
	
	/**
	 * Execute the task in the current thread.
	 * @param p_recordStatistics True to enable statistics recording for the task, false otherwise.
	 * @return True if execution was successful, false otherwise.
	 */
	public boolean executeTask(final boolean p_recordStatistics)
	{
		m_statisticsService.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_execute);
		boolean ret = execute();
		m_statisticsService.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_execute);
		return ret;
	}
	
	/**
	 * Implement this call and add your code to be executed.
	 * @return True if execution was successful, false otherwise.
	 */
	protected abstract boolean execute();
	
	/**
	 * Used by the pipeline set DXRAM and get services.
	 * @param p_dxram DXRAM instance to use.
	 */
	void setDXRAM(final DXRAM p_dxram)
	{
		m_dxram = p_dxram;
		m_loggerService = p_dxram.getService(LoggerService.class);
		m_bootService = p_dxram.getService(BootService.class);
		m_chunkService = p_dxram.getService(ChunkService.class);
		m_jobService = p_dxram.getService(JobService.class);
		m_networkService = p_dxram.getService(NetworkService.class);
		m_statisticsService = p_dxram.getService(StatisticsService.class);
		
		registerStatisticsOperations();
	}	
	
	/**
	 * Register statistics to be recorded by the task itself (task only).
	 */
	private void registerStatisticsOperations() 
	{
		m_statisticsRecorderIDs = new TaskStatisticsRecorderIDs();
		m_statisticsRecorderIDs.m_id = m_statisticsService.createRecorder(this.getClass());
		
		m_statisticsRecorderIDs.m_operations.m_execute = m_statisticsService.createOperation(m_statisticsRecorderIDs.m_id, TaskStatisticsRecorderIDs.Operations.MS_EXECUTE);
	}
}
