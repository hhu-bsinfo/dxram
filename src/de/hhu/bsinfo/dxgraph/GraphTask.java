package de.hhu.bsinfo.dxgraph;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.ChunkStatisticsRecorderIDs;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorder;
import de.hhu.bsinfo.dxram.stats.StatisticsService;

public abstract class GraphTask 
{
	private DXRAM m_dxram = null;
	protected LoggerService m_loggerService = null;
	protected BootService m_bootService = null;
	protected ChunkService m_chunkService = null;
	protected JobService m_jobService = null;
	protected NetworkService m_networkService = null;
	protected StatisticsService m_statisticsService = null;
	
	private GraphTaskStatisticsRecorderIDs m_statisticsRecorderIDs = null;
	
	public GraphTask()
	{
		
	}
	
	public void printStatistics() {
		m_statisticsService.printStatistics(getClass());
	}
	
	public StatisticsRecorder getStatistics() {
		return m_statisticsService.getRecorder(getClass());
	}
	
	public boolean executeTask(final boolean p_recordStatistics)
	{
		m_statisticsService.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_execute);
		boolean ret = execute();
		m_statisticsService.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_execute);
		return ret;
	}
	
	protected abstract boolean execute();
	
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
	
	private void registerStatisticsOperations() 
	{
		m_statisticsRecorderIDs = new GraphTaskStatisticsRecorderIDs();
		m_statisticsRecorderIDs.m_id = m_statisticsService.createRecorder(this.getClass());
		
		m_statisticsRecorderIDs.m_operations.m_execute = m_statisticsService.createOperation(m_statisticsRecorderIDs.m_id, GraphTaskStatisticsRecorderIDs.Operations.MS_EXECUTE);
	}
}
