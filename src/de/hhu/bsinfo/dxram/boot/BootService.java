
package de.hhu.bsinfo.dxram.boot;

import java.net.InetSocketAddress;
import java.util.List;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * Service providing information about the bootstrapping process like
 * node ids, node roles, addresses etc.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class BootService extends AbstractDXRAMService {

	private AbstractBootComponent m_boot;

	/**
	 * Get the ID of the node, you are currently running on.
	 * @return NodeID.
	 */
	public short getNodeID() {
		return m_boot.getNodeID();
	}

	/**
	 * Get IDs of all available (online) nodes including own.
	 * @return List of IDs of nodes available.
	 */
	public List<Short> getIDsOfOnlineNodes() {
		return m_boot.getIDsOfOnlineNodes();
	}

	/**
	 * Get the node role of the current node.
	 * @return Node role of current node.
	 */
	public NodeRole getNodeRole() {
		return m_boot.getNodeRole();
	}

	/**
	 * Get the role of another nodeID.
	 * @param p_nodeID
	 *            Node id to get the role of.
	 * @return Role of other nodeID or null if node does not exist.
	 */
	public NodeRole getNodeRole(final short p_nodeID) {
		return m_boot.getNodeRole(p_nodeID);
	}

	/**
	 * Get the IP and port of another node.
	 * @param p_nodeID
	 *            Node ID of the node.
	 * @return IP and port of the specified node or an invalid address if not available.
	 */
	public InetSocketAddress getNodeAddress(final short p_nodeID) {
		return m_boot.getNodeAddress(p_nodeID);
	}

	/**
	 * Get IDs of all available (online) peer nodes exception our own.
	 * @return List of IDs of nodes available.
	 */
	public List<Short> getAvailablePeerNodeIDs() {
		return m_boot.getIDsOfOnlinePeers();
	}

	/**
	 * Check if a node is available/exists.
	 * @param p_nodeID
	 *            Node ID to check.
	 * @return True if available, false otherwise.
	 */
	public boolean nodeAvailable(final short p_nodeID) {
		return m_boot.nodeAvailable(p_nodeID);
	}

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {

	}

	@Override
	protected boolean startService(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		m_boot = getComponent(AbstractBootComponent.class);
		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_boot = null;
		return true;
	}

}
