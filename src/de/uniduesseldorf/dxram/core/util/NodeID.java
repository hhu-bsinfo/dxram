
package de.uniduesseldorf.dxram.core.util;

import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.Role;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Wrapper class for a NodeID
 * @author Florian Klein
 *         23.07.2013
 */
public final class NodeID {

	// Constants
	public static final short INVALID_ID = -1;

	public static final int MAX_ID = 65535;

	// Attributes
	private static short m_localNodeID;
	private static Role m_role;

	// Constructors
	/**
	 * Creates an instance of NodeID
	 */
	private NodeID() {}

	// Getters
	/**
	 * Get the local NodeID
	 * @return the local NodeID
	 */
	public static short getLocalNodeID() {
		return m_localNodeID;
	}

	/**
	 * Returns the node's role
	 * @return the node's role
	 */
	public static Role getRole() {
		return m_role;
	}

	// Setters
	/**
	 * Set the local NodeID
	 * @param p_localNodeID
	 *            the local NodeID
	 */
	public static void setLocalNodeID(final short p_localNodeID) {
		System.out.println("Own NodeID: " + p_localNodeID + " (" + Long.toHexString(p_localNodeID & 0xFFFF) + ")");

		m_localNodeID = p_localNodeID;
	}

	/**
	 * Sets the role of the node
	 * @param p_role
	 *            the role (Superpeer, Peer or Monitor)
	 */
	public static void setRole(final Role p_role) {
		m_role = p_role;
	}

	// Methods
	/**
	 * Checks if the NodeID is valid
	 * @param p_nodeID
	 *            the NodeID
	 */
	public static void check(final short p_nodeID) {
		Contract.check(p_nodeID != INVALID_ID, "invalid NodeID");
	}

}
