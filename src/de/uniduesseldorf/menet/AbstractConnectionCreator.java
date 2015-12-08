
package de.uniduesseldorf.menet;

import java.io.IOException;

import org.apache.log4j.Logger;

import de.uniduesseldorf.menet.AbstractConnection.DataReceiver;

/**
 * Creates new network connections
 * @author Florian Klein
 *         18.03.2012
 */
abstract class AbstractConnectionCreator {

	// Constants
	protected static final Logger LOGGER = Logger.getLogger(AbstractConnectionCreator.class);

	// Attributes
	private ConnectionCreatorListener m_listener;

	private static AbstractConnectionCreator m_instance;

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
	 * @throws DXRAMException
	 *             if the creator could not be initialized
	 */
	protected void initialize(final short p_nodeID) {}

	/**
	 * Closes the creator and frees unused resources
	 */
	public void close() {}

	/**
	 * Creates a new connection to the given destination
	 * @param p_destination
	 *            the destination
	 * @param p_listener
	 *            the ConnectionListener
	 * @return a new connection
	 * @throws IOException
	 *             if the connection could not be created
	 */
	public abstract AbstractConnection createConnection(short p_destination, DataReceiver p_listener) throws IOException;

	/**
	 * Informs the ConnectionCreatorListener about a new connection
	 * @param p_connection
	 *            the new connection
	 */
	protected final void fireConnectionCreated(final AbstractConnection p_connection) {
		if (m_listener != null) {
			m_listener.connectionCreated(p_connection);
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
		 * A connection was closed
		 * @param p_connection
		 *            the closed connection
		 */
		void connectionClosed(AbstractConnection p_connection);

	}

}
