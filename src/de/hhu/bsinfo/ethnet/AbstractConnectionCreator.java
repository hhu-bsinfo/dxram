
package de.hhu.bsinfo.ethnet;

import java.io.IOException;

/**
 * Creates new network connections
 * @author Florian Klein
 *         18.03.2012
 */
abstract class AbstractConnectionCreator {

	// Attributes
	private ConnectionCreatorListener m_listener;

	// Constructors
	/**
	 * Creates an instance of AbstractConnectionCreator
	 */
	protected AbstractConnectionCreator() {
		m_listener = null;
	}

	// Setters
	/**
	 * Sets the ConnectionCreatorListener
	 * @param p_listener
	 *            the ConnectionCreatorListener
	 */
	public final void setListener(final ConnectionCreatorListener p_listener) {
		m_listener = p_listener;
	}

	// Methods
	/**
	 * Initializes the creator
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_listenPort
	 *            the listen port
	 */
	protected void initialize(final short p_nodeID, final int p_listenPort) {}

	/**
	 * Closes the creator and frees unused resources
	 */
	public void close() {}

	/**
	 * Creates a new connection to the given destination
	 * @param p_destination
	 *            the destination
	 * @return a new connection
	 * @throws IOException
	 *             if the connection could not be created
	 */
	public abstract AbstractConnection createConnection(short p_destination) throws IOException;

	/**
	 * Returns the selector status
	 * @return the selector status
	 */
	public abstract String getSelectorStatus();

	/**
	 * Check if there a remote node tries to open a connection currently
	 * @return whether a remote node opens a connection to this node currently or not
	 */
	public boolean keyIsPending() {
		return true;
	}

	/**
	 * Informs the ConnectionCreatorListener about a new connection
	 * @param p_connection
	 *            the new connection
	 */
	protected final void fireConnectionCreated(final AbstractConnection p_connection) {
		if (m_listener != null) {
			m_listener.connectionCreated(p_connection);
			p_connection.setConnected(true);
		}
	}

	/**
	 * Informs the ConnectionCreatorListener to create a new connection
	 * @param p_destination
	 *            the remote NodeID
	 */
	protected final void fireCreateConnection(final short p_destination) {
		if (m_listener != null) {
			m_listener.createConnection(p_destination);
		}
	}

	/**
	 * Informs the ConnectionCreatorListener about a closed connection
	 * @param p_connection
	 *            the closed connection
	 */
	protected final void fireConnectionClosed(final AbstractConnection p_connection) {
		if (m_listener != null) {
			m_listener.connectionClosed(p_connection);
		}
	}

	// Classes
	/**
	 * Methods for reacting to new or closed connections
	 * @author Florian Klein
	 *         18.03.2012
	 */
	public interface ConnectionCreatorListener {

		// Methods
		/**
		 * A new connection was created
		 * @param p_connection
		 *            the new connection
		 */
		void connectionCreated(AbstractConnection p_connection);

		/**
		 * A new connection must be created
		 * @param p_destination
		 *            the remote NodeID
		 */
		void createConnection(short p_destination);

		/**
		 * A connection was closed
		 * @param p_connection
		 *            the closed connection
		 */
		void connectionClosed(AbstractConnection p_connection);

	}

}
