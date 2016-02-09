package de.hhu.bsinfo.dxram.boot;

import de.hhu.bsinfo.dxram.engine.DXRAMService;

public class BootService extends DXRAMService {

	private BootComponent m_boot = null;
	
	/**
	 * Get the ID of the node, you are currently running on.
	 * @return NodeID.
	 */
	public short getNodeID() {
		return m_boot.getNodeID();
	}
	
	@Override
	protected void registerDefaultSettingsService(Settings p_settings) {

	}

	@Override
	protected boolean startService(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {
		m_boot = getComponent(BootComponent.class);
		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_boot = null;
		return true;
	}

}
