
package de.hhu.bsinfo.dxram.net.events;

import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * An event for a delayed request. Triggered by application thread.
 * Indicates a problem, only.
 * @author Kevin Beineke <kevin.beineke@hhu.de> 05.10.16
 */
public class ResponseDelayedEvent extends AbstractEvent {

	private short m_nodeID = NodeID.INVALID_ID;

	/**
	 * Creates an instance of NodeFailureEvent
	 * @param p_sourceClass
	 *            the calling class
	 * @param p_nodeID
	 *            the NodeID of the failed peer
	 */
	public ResponseDelayedEvent(final String p_sourceClass, final short p_nodeID) {
		super(p_sourceClass);

		m_nodeID = p_nodeID;
	}

	/**
	 * Returns the NodeID
	 * @return the failed peer's NodeID
	 */
	public short getNodeID() {
		return m_nodeID;
	}
}
