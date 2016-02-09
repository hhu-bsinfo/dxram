package de.hhu.bsinfo.dxgraph;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.job.Job;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.logger.LoggerService;

public abstract class GraphTask 
{
	private DXRAM m_dxram = null;
	private LoggerService m_loggerService = null;
	private BootService m_bootService = null;
	private ChunkService m_chunkService = null;
	private JobService m_jobService = null;
	
	public GraphTask()
	{
		
	}
	
	public abstract boolean execute();
	
	void setDXRAM(final DXRAM p_dxram)
	{
		m_dxram = p_dxram;
		m_loggerService = p_dxram.getService(LoggerService.class);
		m_bootService = p_dxram.getService(BootService.class);
		m_chunkService = p_dxram.getService(ChunkService.class);
		m_jobService = p_dxram.getService(JobService.class);
	}
	
	protected short getNodeID()
	{
		return m_bootService.getNodeID();
	}
	
	protected void log(final String p_msg)
	{
		m_loggerService.debug(getClass(), p_msg);
	}
	
	protected void logError(final String p_msg)
	{
		m_loggerService.error(getClass(), p_msg);
	}
	
	protected int createData(final DataStructure... p_dataStructures)
	{
		return m_chunkService.create(p_dataStructures);
	}
	
	protected int getData(final DataStructure... p_dataStructures)
	{
		return m_chunkService.get(p_dataStructures);
	}
	
	protected int putData(final DataStructure... p_dataStructures)
	{
		return m_chunkService.put(p_dataStructures);
	}
	
	protected void pushJob(final Job p_job)
	{
		m_jobService.pushJob(p_job);
	}
	
	protected boolean waitForLocalJobsToFinish()
	{
		return m_jobService.waitForLocalJobsToFinish();
	}
}
