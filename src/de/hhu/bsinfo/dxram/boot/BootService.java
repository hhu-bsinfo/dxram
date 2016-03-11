package de.hhu.bsinfo.dxram.boot;

import java.net.InetSocketAddress;
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
	 * Get the node role of the current node.
	 * @return Node role of current node.
	 */
	public NodeRole getNodeRole() 
	{
		return m_boot.getNodeRole();
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
	 * Get the IP and port of another node.
	 * @param p_nodeID Node ID of the node.
	 * @return IP and port of the specified node or an invalid address if not available.
	 */
	public InetSocketAddress getNodeAddress(final short p_nodeID)
	{
		return m_boot.getNodeAddress(p_nodeID);
	}
	
	/**
	 * Get IDs of all available (online) peer nodes.
	 * @return List of IDs of nodes available.
	 */
	public List<Short> getAvailablePeerNodeIDs()
	{
		return m_boot.getAvailableNodeIDs();
	}
	
	/**
	 * Check if a node is available/exists.
	 * @param p_nodeID Node ID to check.
	 * @return True if available, false otherwise.
	 */
	public boolean nodeAvailable(final short p_nodeID)
	{
		return m_boot.nodeAvailable(p_nodeID);
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
