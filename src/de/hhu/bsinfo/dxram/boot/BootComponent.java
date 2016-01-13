package de.hhu.bsinfo.dxram.boot;

import java.net.InetSocketAddress;

import de.hhu.bsinfo.dxram.engine.DXRAMComponent;

public abstract class BootComponent extends DXRAMComponent {

	public BootComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

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
	 * Get the IP and port of another node.
	 * @param p_nodeID Node ID of the node.
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
	 * @return Node ID assigned for bootstrapping.
	 */
	public abstract short getNodeIDBootstrap();
	
	/**
	 * Check if a node is available/exists.
	 * @param p_nodeID Node ID to check.
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
	 * @param p_failedNode
	 *            the failed node
	 * @param p_isSuperpeer
	 *            whether the failed node was a superpeer or not
	 * @return true if the current node reported the failure first
	 */
	public abstract boolean reportNodeFailure(final short p_nodeID, final boolean p_isSuperpeer);
	
	/**
	 * Promote the current node to a superpeer.
	 * @return True if promotion was successful, false otherwise.
	 */
	public abstract boolean promoteToSuperpeer();
	
	/**
	 * Demote the current node to a peer.
	 * @return True if demotion was successful, false otherwise.
	 */
	public abstract boolean demoteToPeer();
}
