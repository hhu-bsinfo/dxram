
package de.uniduesseldorf.dxram.core.exceptions;

import de.uniduesseldorf.dxram.core.api.NodeID;

/**
 * Exception for failed logging accesses
 * @author Kevin Beineke
 *         26.05.2014
 */
public class SecondaryLogException extends DXRAMException {

	// Constants
	private static final long serialVersionUID = -1606349411876483752L;

	// Attributes
	private final short m_nodeID;
	private final int m_dataSize;

	// Constructors
	/**
	 * Creates an instance of LogException
	 * @param p_message
	 *            the message
	 */
	public SecondaryLogException(final String p_message) {
		super(p_message);
		m_nodeID = NodeID.INVALID_ID;
		m_dataSize = 0;
	}

	/**
	 * Creates an instance of LogException
	 * @param p_message
	 *            the message
	 * @param p_cause
	 *            the cause
	 */
	public SecondaryLogException(final String p_message, final Throwable p_cause) {
		super(p_message, p_cause);
		m_nodeID = NodeID.INVALID_ID;
		m_dataSize = 0;
	}

	/**
	 * Creates an instance of LogException
	 * @param p_message
	 *            the message
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_dataSize
	 *            the data size
	 */
	public SecondaryLogException(final String p_message, final short p_nodeID, final int p_dataSize) {
		super(p_message);
		m_nodeID = p_nodeID;
		m_dataSize = p_dataSize;
	}


	// Getters
	/**
	 * Returns the NodeID
	 * @return the NodeID
	 */
	public final short getNodeID() {
		return m_nodeID;
	}

	/**
	 * Returns the data size
	 * @return the data size
	 */
	public final int getDataSize() {
		return m_dataSize;
	}
}
