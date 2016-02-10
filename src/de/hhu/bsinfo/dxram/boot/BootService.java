package de.hhu.bsinfo.dxram.boot;

import java.util.List;

import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.util.NodeRole;

public class BootService extends DXRAMService {

	private BootComponent m_boot = null;
	
	/**
	 * Get the ID of the node, you are currently running on.
	 * @return NodeID.
	 */
	public short getNodeID() {
		return m_boot.getNodeID();
	}
	
	/**
	 * Get IDs of all available (online) nodes.
	 * @return List of IDs of nodes available.
	 */
	public List<Short> getAvailableNodeIDs()
	{
		return m_boot.getAvailableNodeIDs();
	}
	
	/**
	 * Get the role of another nodeID.
	 * @return Role of other nodeID or null if node does not exist.
	 */
	public NodeRole getNodeRole(final short p_nodeID)
	{
		return m_boot.getNodeRole(p_nodeID);
	}
	
	/**
	 * Get IDs of all available (online) peer nodes.
	 * @return List of IDs of nodes available.
	 */
	public List<Short> getAvailablePeerNodeIDs()
	{
		return m_boot.getAvailableNodeIDs();
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
