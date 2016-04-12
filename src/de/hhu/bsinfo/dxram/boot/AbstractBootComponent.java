
package de.hhu.bsinfo.dxram.boot;

import java.net.InetSocketAddress;
import java.util.List;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * Component executing the bootstrapping of a node in DXRAM.
 * It takes care of assigning the node ID to this node, its role and
 * managing everything related to the basic node status (available,
 * failure report...)
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public abstract class AbstractBootComponent extends AbstractDXRAMComponent {

	/**
	 * Constructor
	 * @param p_priorityInit
	 *            Priority for initialization of this component.
	 *            When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown
	 *            Priority for shutting down this component.
	 *            When choosing the order, consider component dependencies here.
	 */
	public AbstractBootComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	/**
	 * Get IDs of all available (online) nodes.
	 * @return List of IDs of nodes available.
	 */
	public abstract List<Short> getIDsOfOnlineNodes();

	/**
	 * Get IDs of all available (online) peer nodes except the own.
	 * @return List of IDs of nodes available without own ID.
	 */
	public abstract List<Short> getIDsOfOnlinePeers();

	/**
	 * Get the node ID, which is currently assigned to this running instance.
	 * @return Own NodeID.
	 */
	public abstract short getNodeID();

	/**
	 * Get the role, which is currently assigned to this running instance.
	 * @return Own Role.
	 */
	public abstract NodeRole getNodeRole();

	/**
	 * Get the role of another nodeID.
	 * @param p_nodeID
	 *            Node id of the node.
	 * @return Role of other nodeID or null if node does not exist.
	 */
	public abstract NodeRole getNodeRole(final short p_nodeID);

	/**
	 * Get the IP and port of another node.
	 * @param p_nodeID
	 *            Node ID of the node.
	 * @return IP and port of the specified node or an invalid address if not available.
	 */
	public abstract InetSocketAddress getNodeAddress(final short p_nodeID);

	/**
	 * Get the number of currently available superpeers.
	 * @return Number of currently available superpeers.
	 */
	public abstract int getNumberOfAvailableSuperpeers();

	/**
	 * Get the node ID of the currently set bootstrap node.
	 * @return Node ID assigned for bootstrapping or -1 if no bootstrap assigned/available.
	 */
	public abstract short getNodeIDBootstrap();

	/**
	 * Check if a node is available/exists.
	 * @param p_nodeID
	 *            Node ID to check.
	 * @return True if available, false otherwise.
	 */
	public abstract boolean nodeAvailable(final short p_nodeID);

	/**
	 * Replaces the current bootstrap with p_nodeID if the failed bootstrap has not been replaced by another superpeer
	 * @param p_nodeID
	 *            the new bootstrap candidate
	 * @return the new bootstrap
	 */
	public abstract short setBootstrapPeer(final short p_nodeID);

	/**
	 * Report that we detected a node failure.
	 * @param p_nodeID
	 *            the failed node
	 * @param p_isSuperpeer
	 *            whether the failed node was a superpeer or not
	 * @return true if the current node reported the failure first
	 */
	public abstract boolean reportNodeFailure(final short p_nodeID, final boolean p_isSuperpeer);
}
