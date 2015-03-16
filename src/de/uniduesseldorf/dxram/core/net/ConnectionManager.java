package de.uniduesseldorf.dxram.core.net;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import de.uniduesseldorf.dxram.core.net.AbstractConnection.DataReceiver;
import de.uniduesseldorf.dxram.core.net.AbstractConnectionCreator.ConnectionCreatorListener;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Manages the network connections
 * @author Florian Klein 18.03.2012
 */
final class ConnectionManager implements ConnectionCreatorListener {

	// Constants
	private static final int MAX_CONNECTIONS = 100;

	// Attributes
	private Map<Short, AbstractConnection> m_connections;

	private AbstractConnectionCreator m_creator;
	private DataReceiver m_connectionListener;

	private boolean m_deactivated;

	// Constructors
	/**
	 * Creates an instance of ConnectionStore
	 * @param p_creator
	 *            the ConnectionCreator
	 */
	public ConnectionManager(final AbstractConnectionCreator p_creator) {
		this(p_creator, null);
	}

	/**
	 * Creates an instance of ConnectionStore
	 * @param p_creator
	 *            the ConnectionCreator
	 * @param p_listener
	 *            the ConnectionListener
	 */
	public ConnectionManager(final AbstractConnectionCreator p_creator, final DataReceiver p_listener) {
		m_connections = new HashMap<>();

		m_creator = p_creator;
		m_creator.setListener(this);
		m_connectionListener = p_listener;
		m_deactivated = false;
	}

	/**
	 * Activates the connection manager
	 */
	public synchronized void activate() {
		m_deactivated = false;
	}

	/**
	 * Deactivates the connection manager
	 */
	public synchronized void deactivate() {
		m_deactivated = true;
	}

	// Methods
	/**
	 * Get the connection for the given destination
	 * @param p_destination
	 *            the destination
	 * @return the connection
	 * @throws IOException
	 *             if the connection could not be get
	 */
	public synchronized AbstractConnection getConnection(final short p_destination) throws IOException {
		AbstractConnection ret;

		Contract.checkNotNull(p_destination, "no destination given");

		ret = m_connections.get(p_destination);
		if (ret == null && !m_deactivated) {
			if (m_connections.size() == MAX_CONNECTIONS) {
				dismissConnection();
			}

			ret = m_creator.createConnection(p_destination, m_connectionListener);
			if (null != ret) {
				m_connections.put(ret.getDestination(), ret);
			}
		}

		return ret;
	}

	/**
	 * Dismiss the connection with the lowest rating
	 */
	private synchronized void dismissConnection() {
		AbstractConnection dismiss = null;
		int lowestRating = Integer.MAX_VALUE;

		for (AbstractConnection connection : m_connections.values()) {
			if (connection.getRating() < lowestRating) {
				dismiss = connection;
				lowestRating = connection.getRating();
			}
		}

		if (dismiss != null) {
			dismiss.close();
		}
	}

	/**
	 * Closes the connection for the given destination
	 * @param p_destination
	 *            the destination
	 */
	public synchronized void closeConnection(final short p_destination) {
		AbstractConnection connection;

		Contract.checkNotNull(p_destination, "no destination given");

		connection = m_connections.get(p_destination);
		if (connection != null) {
			connection.close();
		}
	}

	/**
	 * A new connection was created
	 * @param p_connection
	 *            the new connection
	 */
	@Override
	public synchronized void connectionCreated(final AbstractConnection p_connection) {
		if (!m_connections.containsKey(p_connection.getDestination())) {
			m_connections.put(p_connection.getDestination(), p_connection);
			p_connection.setListener(m_connectionListener);
		}
	}

	/**
	 * A connection was closed
	 * @param p_connection
	 *            the closed connection
	 */
	@Override
	public synchronized void connectionClosed(final AbstractConnection p_connection) {
		if (m_connections.containsKey(p_connection.getDestination())) {
			m_connections.remove(p_connection.getDestination());
			p_connection.cleanup();
			// TODO: Inform and update system
		}
	}

}
