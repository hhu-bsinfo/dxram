package de.uniduesseldorf.dxram.core.boot;

import de.uniduesseldorf.dxram.core.engine.DXRAMComponent;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesWatcher;

// TODO put zookeeper stuff in here
public class BootComponent extends DXRAMComponent {

	public static final String COMPONENT_IDENTIFIER = "Boot";
	
	private NodesWatcher m_nodesWatcher = null;
	
	public BootComponent(int p_priorityInit, int p_priorityShutdown) {
		super(COMPONENT_IDENTIFIER, p_priorityInit, p_priorityShutdown);
	}

	@Override
	protected void registerDefaultSettingsComponent(Settings p_settings) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected boolean initComponent(Settings p_settings) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected boolean shutdownComponent() {
		// TODO Auto-generated method stub
		return false;
	}

}
