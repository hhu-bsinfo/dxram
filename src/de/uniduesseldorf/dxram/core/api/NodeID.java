
package de.uniduesseldorf.dxram.core.api;

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
	private static boolean m_superpeer;

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
	 * Checks if the local node is a superpeer
	 * @return true if the local node is a superpeer, false otherwise
	 */
	public static boolean isSuperpeer() {
		return m_superpeer;
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
	 * Determines if the local node is a peer or a superpper
	 * @param p_superpeer
	 *            true if the the local node is a superpeer, false if the local node is only a peer
	 */
	public static void setSuperpeer(final boolean p_superpeer) {
		m_superpeer = p_superpeer;
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
