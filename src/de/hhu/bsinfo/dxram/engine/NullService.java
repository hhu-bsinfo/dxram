
package de.hhu.bsinfo.dxram.engine;

/**
 * Dummy service implementation for testing.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class NullService extends AbstractDXRAMService {

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {

	}

	@Override
	protected boolean startService(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		return true;
	}

	@Override
	protected boolean shutdownService() {
		return true;
	}
}
