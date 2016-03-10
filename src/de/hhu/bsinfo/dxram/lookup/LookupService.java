package de.hhu.bsinfo.dxram.lookup;

import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.engine.DXRAMService;

// TODO move many things from the LookupComponent to this class:
// - network message handling
// - connection lost handling
public class LookupService extends DXRAMService {
	
	public LookupService() {
	}

	@Override
	protected void registerDefaultSettingsService(Settings p_settings) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	protected boolean startService(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected boolean shutdownService() {
		// TODO Auto-generated method stub
		return false;
	}
	
}
