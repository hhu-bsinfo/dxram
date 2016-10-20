
package de.hhu.bsinfo.dxram.failure.events;

import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * An event for node failure
 * @author Kevin Beineke <kevin.beineke@hhu.de> 30.03.16
 */
public class NodeFailureEvent extends AbstractEvent {

	private short m_nodeID = NodeID.INVALID_ID;
	private NodeRole m_role = NodeRole.PEER;

	/**
	 * Creates an instance of NodeFailureEvent
	 * @param p_sourceClass
	 *            the calling class
	 * @param p_nodeID
	 *            the NodeID of the failed peer
	 * @param p_role
	 *            the failed peer's role
	 */
	public NodeFailureEvent(final String p_sourceClass, final short p_nodeID, final NodeRole p_role) {
		super(p_sourceClass);

		m_nodeID = p_nodeID;
		m_role = p_role;
	}

	/**
	 * Returns the NodeID
	 * @return the failed peer's NodeID
	 */
	public short getNodeID() {
		return m_nodeID;
	}

	/**
	 * Returns the NodeRole
	 * @return the failed peer's role
	 */
	public NodeRole getRole() {
		return m_role;
	}
}
