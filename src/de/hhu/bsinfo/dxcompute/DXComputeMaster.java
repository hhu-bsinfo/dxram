package de.hhu.bsinfo.dxcompute;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.logger.LoggerService;

public class DXComputeMaster
{
	private DXRAM m_dxram;
	private LoggerService m_loggerService;

	public DXCompute(final DXRAM p_dxram) {
		m_dxram = p_dxram;
		m_loggerService = m_dxram.getService(LoggerService.class);
	}

	public boolean executePipeline(final Pipeline p_pipeline) {
		p_pipeline.setDXRAM(m_dxram);

		if (!p_pipeline.execute()) {
			m_loggerService.error(getClass(), "Executing pipeline " + p_pipeline + " failed.");
			return false;
		}

		return true;
	}
}
