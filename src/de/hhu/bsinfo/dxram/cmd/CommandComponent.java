package de.hhu.bsinfo.dxram.cmd;

import de.hhu.bsinfo.dxram.engine.DXRAMComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;

public class CommandComponent extends DXRAMComponent {

	private LoggerComponent m_logger = null;
	private NetworkComponent m_network = null;
	
	public CommandComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void registerDefaultSettingsComponent(Settings p_settings) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected boolean initComponent(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected boolean shutdownComponent() {
		// TODO Auto-generated method stub
		return false;
	}

}
