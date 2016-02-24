
package de.uniduesseldorf.dxram.core.net;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

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

	private ReentrantLock m_lock;

	// Constructors
	/**
	 * Creates an instance of ConnectionStore
	 * @param p_creator
	 *            the ConnectionCreator
	 */
	ConnectionManager(final AbstractConnectionCreator p_creator) {
		this(p_creator, null);
	}

	/**
	 * Creates an instance of ConnectionStore
	 * @param p_creator
	 *            the ConnectionCreator
	 * @param p_listener
	 *            the ConnectionListener
	 */
	ConnectionManager(final AbstractConnectionCreator p_creator, final DataReceiver p_listener) {
		m_connections = new HashMap<>();

		m_creator = p_creator;
		m_creator.setListener(this);
		m_connectionListener = p_listener;
		m_deactivated = false;

		m_lock = new ReentrantLock(false);
	}

	/**
	 * Activates the connection manager
	 */
	public void activate() {
		m_lock.lock();
		m_deactivated = false;
		m_lock.unlock();
	}

	/**
	 * Deactivates the connection manager
	 */
	public void deactivate() {
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
	public AbstractConnection getConnection(final short p_destination) throws IOException {
		AbstractConnection ret;

		Contract.checkNotNull(p_destination, "no destination given");

		m_lock.lock();
		try {
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
		} catch (final IOException e) {
			m_lock.unlock();
			throw e;
		}
		m_lock.unlock();

		return ret;
	}

	/**
	 * Dismiss the connection randomly
	 */
	@SuppressWarnings("unchecked")
	private void dismissConnection() {
		AbstractConnection dismiss = null;
		Random rand;

		rand = new Random();
		m_lock.lock();
		dismiss = ((Entry<Short, AbstractConnection>[]) m_connections.entrySet().toArray())[rand.nextInt(m_connections.size())].getValue();

		if (dismiss != null) {
			dismiss.close();
		}
		m_lock.unlock();
	}

	/**
	 * Closes the connection for the given destination
	 * @param p_destination
	 *            the destination
	 */
	public void closeConnection(final short p_destination) {
		AbstractConnection connection;

		Contract.checkNotNull(p_destination, "no destination given");

		m_lock.lock();
		connection = m_connections.get(p_destination);
		if (connection != null) {
			connection.close();
		}
		m_lock.unlock();
	}

	/**
	 * A new connection was created
	 * @param p_connection
	 *            the new connection
	 */
	@Override
	public void connectionCreated(final AbstractConnection p_connection) {
		m_lock.lock();
		if (!m_connections.containsKey(p_connection.getDestination())) {
			m_connections.put(p_connection.getDestination(), p_connection);
			p_connection.setListener(m_connectionListener);
		}
		m_lock.unlock();
	}

	/**
	 * A connection was closed
	 * @param p_connection
	 *            the closed connection
	 */
	@Override
	public void connectionClosed(final AbstractConnection p_connection) {
		m_lock.lock();
		if (m_connections.containsKey(p_connection.getDestination())) {
			m_connections.remove(p_connection.getDestination());
			p_connection.cleanup();
			// TODO: Inform and update system
		}
		m_lock.unlock();
	}

}
