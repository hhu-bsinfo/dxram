package de.hhu.bsinfo.dxram;

import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.engine.DXRAMService;

public final class DXRAM 
{
	private DXRAMEngine m_engine;
	
	public DXRAM()
	{
		m_engine = new DXRAMEngine();
	}
	
	public boolean initialize(final String p_configurationFolder) {
		return initialize(p_configurationFolder, null, null, null);
	}
	
	public boolean initialize(final String p_configurationFolder, final String p_overrideIp, 
			final String p_overridePort, final String p_overrideRole) {
		return m_engine.init(p_configurationFolder);
	}
	
	public <T extends DXRAMService> T getService(final Class<T> p_class) {		   
		return m_engine.getService(p_class);
	}

	public void shutdown() {
		m_engine.shutdown();
	}
}
