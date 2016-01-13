package de.hhu.bsinfo.dxram.engine;

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
