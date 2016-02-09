package de.hhu.bsinfo.dxgraph;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.logger.LoggerService;

// some kind of "wrapper class around DXRAM to provide
// simple to use functions for graph operations
// TODO have option to execute pipeline in separate thread
public class DXGraph 
{
	private DXRAM m_dxram = null;
	private LoggerService m_loggerService = null;
	
	public DXGraph(final DXRAM p_dxram)
	{
		m_dxram = p_dxram;
		m_loggerService = m_dxram.getService(LoggerService.class);
	}
	
	public boolean executePipeline(final GraphTaskPipeline p_pipeline)
	{
		p_pipeline.setDXRAM(m_dxram);
		if (!p_pipeline.setup())
		{
			m_loggerService.error(getClass(), "Setting up pipeline " + p_pipeline + " failed.");
			return false;
		}
		
		if (!p_pipeline.execute())
		{
			m_loggerService.error(getClass(), "Executing pipeline " + p_pipeline + " failed.");
			return false;
		}
		
		return true;
	}
}
