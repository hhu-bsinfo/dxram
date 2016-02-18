package de.hhu.bsinfo.dxcompute;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.logger.LoggerService;

/**
 * DXCompute main class. This takes DXRAM and provides features to enable
 * computing of multiple tasks contained in a single pipeline.
 * 
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 */
public class DXCompute 
{
	private DXRAM m_dxram = null;
	private LoggerService m_loggerService = null;

	/**
	 * Constructor
	 * @param p_dxram DXRAM instance to use for computing.
	 */
	public DXCompute(final DXRAM p_dxram)
	{
		m_dxram = p_dxram;
		m_loggerService = m_dxram.getService(LoggerService.class);
	}
	
	// TODO have queue to schedule pipelines?
	
	/**
	 * Execute a pipeline with the current thread.
	 * @param p_pipeline Pipeline to execute.
	 * @return True if execution was successful, false otherwise.
	 */
	public boolean executePipeline(final Pipeline p_pipeline)
	{
		p_pipeline.setDXRAM(m_dxram);
		
		if (!p_pipeline.execute())
		{
			m_loggerService.error(getClass(), "Executing pipeline " + p_pipeline + " failed.");
			return false;
		}
		
		return true;
	}
}
