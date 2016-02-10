package de.hhu.bsinfo.dxgraph;

import java.util.List;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.job.JobService;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.net.NetworkService;

public abstract class GraphTask 
{
	private DXRAM m_dxram = null;
	protected LoggerService m_loggerService = null;
	protected BootService m_bootService = null;
	protected ChunkService m_chunkService = null;
	protected JobService m_jobService = null;
	protected NetworkService m_networkService = null;
	
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
}
