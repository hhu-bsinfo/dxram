
package de.uniduesseldorf.menet;

import de.uniduesseldorf.utils.Contract;

/**
 * Wrapper class for a NodeID
 * @author Florian Klein
 *         23.07.2013
 */
public final class NodeID {

	// Constants
	public static final short INVALID_ID = -1;

	public static final int MAX_ID = 65535;

	// Constructors
	/**
	 * Creates an instance of NodeID
	 */
	private NodeID() {}

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
