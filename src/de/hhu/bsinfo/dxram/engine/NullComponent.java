package de.hhu.bsinfo.dxram.engine;

public class NullComponent extends DXRAMComponent {

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
