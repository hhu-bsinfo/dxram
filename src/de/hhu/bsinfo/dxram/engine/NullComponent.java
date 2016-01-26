package de.hhu.bsinfo.dxram.engine;

/**
 * Dummy component implementation for testing.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class NullComponent extends DXRAMComponent {

	/**
	 * Constructor
	 * @param p_priorityInit Priority for initialization of this component. 
	 * 			When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown Priority for shutting down this component. 
	 * 			When choosing the order, consider component dependencies here.
	 */
	public NullComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	@Override
	protected void registerDefaultSettingsComponent(Settings p_settings) {

	}

	@Override
	protected boolean initComponent(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		return true;
	}
}
