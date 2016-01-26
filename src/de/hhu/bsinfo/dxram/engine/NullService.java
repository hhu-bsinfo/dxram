package de.hhu.bsinfo.dxram.engine;

/**
 * Dummy service implementation for testing.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class NullService extends DXRAMService {

	@Override
	protected void registerDefaultSettingsService(Settings p_settings) {
	}

	@Override
	protected boolean startService(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {
		return true;
	}

	@Override
	protected boolean shutdownService() {
		return true;
	}
}
