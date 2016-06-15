
package de.hhu.bsinfo.menet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.menet.AbstractConnection.DataReceiver;
import de.hhu.bsinfo.menet.AbstractConnectionCreator.ConnectionCreatorListener;

/**
 * Manages the network connections
 * @author Florian Klein 18.03.2012
 */
public final class ConnectionManager implements ConnectionCreatorListener {

	// Constants
	private static final int MAX_CONNECTIONS = 100;

	// Attributes
	private AbstractConnection[] m_connections;
	private ArrayList<AbstractConnection> m_connectionList;

	private AbstractConnectionCreator m_creator;
	private DataReceiver m_connectionListener;

	private short m_ownNodeID;
	private boolean m_deactivated;

	private ReentrantLock m_incomingOutgoingLock;
	private Condition m_cond;
	private ReentrantLock m_applicationThreadLock;

	// Constructors
	/**
	 * Creates an instance of ConnectionStore
	 * @param p_creator
	 *            the ConnectionCreator
	 * @param p_listener
	 *            the ConnectionListener
	 * @param p_ownNodeID
	 *            the own NodeID needed for connection duplicate consensus
	 */
	ConnectionManager(final AbstractConnectionCreator p_creator, final DataReceiver p_listener, final short p_ownNodeID) {
		m_connections = new AbstractConnection[65536];
		m_connectionList = new ArrayList<AbstractConnection>(65536);

		m_creator = p_creator;
		m_creator.setListener(this);
		m_connectionListener = p_listener;

		m_ownNodeID = p_ownNodeID;
		m_deactivated = false;

		m_incomingOutgoingLock = new ReentrantLock(false);
		m_applicationThreadLock = new ReentrantLock(false);
		m_cond = m_incomingOutgoingLock.newCondition();
	}

	/**
	 * Closes the ConnectionManager
	 */
	public void close() {
		m_creator.close();
	}

	/**
	 * Returns the status of all connections
	 * @return the statuses
	 */
	public String getConnectionStatuses() {
		String ret = "";

		m_incomingOutgoingLock.lock();
		Iterator<AbstractConnection> iter = m_connectionList.iterator();
		while (iter.hasNext()) {
			ret += iter.next().toString();
		}
		m_incomingOutgoingLock.unlock();

		return ret;
	}

	/**
	 * Checks if there is a congested connection
	 * @return whether there is congested connection or not
	 */
	public boolean atLeastOneConnectionIsCongested() {
		boolean ret = false;

		m_incomingOutgoingLock.lock();
		Iterator<AbstractConnection> iter = m_connectionList.iterator();
		while (iter.hasNext()) {
			if (iter.next().isCongested()) {
				ret = true;
				break;
			}
		}
		m_incomingOutgoingLock.unlock();

		return ret;
	}

	/**
	 * Activates the connection manager
	 */
	public void activate() {
		m_incomingOutgoingLock.lock();
		m_deactivated = false;
		m_incomingOutgoingLock.unlock();
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

		assert p_destination != NodeID.INVALID_ID;

		ret = m_connections[p_destination & 0xFFFF];
		if (ret == null && !m_deactivated) {
			m_applicationThreadLock.lock();
			m_incomingOutgoingLock.lock();

			ret = m_connections[p_destination & 0xFFFF];
			if (ret == null && !m_deactivated) {

				while (m_creator.keyIsPending()) {
					// System.out.println("Key is pending -> waiting");
					try {
						m_cond.await();
					} catch (final InterruptedException e) {}
				}

				ret = m_connections[p_destination & 0xFFFF];
				if (ret == null && !m_deactivated) {
					if (m_connectionList.size() == MAX_CONNECTIONS) {
						dismissRandomConnection();
					}
					m_incomingOutgoingLock.unlock();

					try {
						ret = m_creator.createConnection(p_destination, m_connectionListener);
					} catch (final IOException e) {
						throw e;
					}

					m_incomingOutgoingLock.lock();
					if (null != ret) {
						// System.out.println("Created outgoing connection to " + NodeID.toHexString(p_destination) + ", " + System.currentTimeMillis());
						addConnection(ret, false);
					}
				}
			}

			m_incomingOutgoingLock.unlock();
			m_applicationThreadLock.unlock();
		}

		return ret;
	}

	/**
	 * Add a new connection. Use duplicate consensus if there is already a connection for the specific NodeID.
	 * @param p_connection
	 *            the new connection
	 * @param p_isIncoming
	 *            whether the new connection's creation was initialized by remote node or not
	 */
	private void addConnection(final AbstractConnection p_connection, final boolean p_isIncoming) {
		short remoteNodeID;
		AbstractConnection connection;

		remoteNodeID = p_connection.getDestination();
		connection = m_connections[remoteNodeID & 0xFFFF];
		if (connection == null) {

			// System.out.println("No connection registered for " + NodeID.toHexString(remoteNodeID));

			// No entry for this NodeID -> insert connection
			m_connections[remoteNodeID & 0xFFFF] = p_connection;
			m_connectionList.add(p_connection);
			p_connection.setListener(m_connectionListener);
		} else {

			System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Collision with already established connection!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

			// There is already a connection for this destination -> connection duplicate consensus
			if (remoteNodeID > m_ownNodeID) {
				// Use the remote node's connection as its NodeID is greater
				if (p_isIncoming) {

					// System.out.println("   Overwriting with new connection initiated by " + NodeID.toHexString(remoteNodeID) + ", " +
					// System.currentTimeMillis());

					// Overwrite the connection as p_connection was initiated by the remote node
					m_connections[remoteNodeID & 0xFFFF] = p_connection;
					m_connectionList.add(p_connection);
					p_connection.setListener(m_connectionListener);

					// System.out.println("   Old connection: " + connection);

					// Close old connection
					connection.close();
					connection.cleanup();
				} else {
					// Keep the connection as its creation was initiated by the remote node

					// System.out.println("Keeping the connection initiated by " + NodeID.toHexString(remoteNodeID) + ", " + System.currentTimeMillis());

					// System.out.println("   Old connection: " + p_connection);

					// Close new connection
					p_connection.close();
					p_connection.cleanup();
				}
			} else {
				// Use this node's connection as this node has a greater NodeID
				if (!p_isIncoming) {

					// System.out.println("Overwriting with new connection initiated by " + NodeID.toHexString(m_ownNodeID) + ", " +
					// System.currentTimeMillis());

					// Overwrite the connection as p_connection was initiated by this node
					m_connections[remoteNodeID & 0xFFFF] = p_connection;
					m_connectionList.add(p_connection);
					p_connection.setListener(m_connectionListener);

					// System.out.println("   Old connection: " + connection);

					// Close old connection
					connection.close();
					connection.cleanup();
				} else {
					// Keep the connection as its creation was initiated by this node

					// System.out.println("Keeping the connection initiated by " + NodeID.toHexString(m_ownNodeID) + ", " + System.currentTimeMillis());

					// System.out.println("   Old connection: " + p_connection);

					// Close new connection
					p_connection.close();
					p_connection.cleanup();
				}
			}
		}
	}

	/**
	 * Dismiss the connection randomly
	 */
	private void dismissRandomConnection() {
		int random = -1;
		AbstractConnection dismiss = null;
		Random rand;

		rand = new Random();
		m_incomingOutgoingLock.lock();
		while (dismiss == null) {
			random = rand.nextInt(m_connections.length);
			dismiss = m_connections[random];
		}

		m_connections[random] = null;
		m_connectionList.remove(dismiss);
		dismiss.close();
		m_incomingOutgoingLock.unlock();
	}

	/**
	 * Closes the connection for the given destination
	 * @param p_destination
	 *            the destination
	 */
	public void closeConnection(final short p_destination) {
		AbstractConnection connection;

		assert p_destination != NodeID.INVALID_ID;

		m_incomingOutgoingLock.lock();
		connection = m_connections[p_destination & 0xFFFF];
		m_connections[p_destination & 0xFFFF] = null;
		m_connectionList.remove(connection);
		if (connection != null) {
			connection.close();
		}
		m_incomingOutgoingLock.unlock();
	}

	/**
	 * A new connection was created
	 * @param p_connection
	 *            the new connection
	 */
	@Override
	public void connectionCreated(final AbstractConnection p_connection) {
		m_incomingOutgoingLock.lock();
		// System.out.println("Created incoming connection to " + NodeID.toHexString(p_connection.getDestination()) + ", " + System.currentTimeMillis());
		addConnection(p_connection, true);
		m_cond.signalAll();
		m_incomingOutgoingLock.unlock();
	}

	/**
	 * A connection was closed
	 * @param p_connection
	 *            the closed connection
	 */
	@Override
	public void connectionClosed(final AbstractConnection p_connection) {
		AbstractConnection connection;

		m_incomingOutgoingLock.lock();
		connection = m_connections[p_connection.getDestination() & 0xFFFF];
		if (connection != null) {
			m_connections[p_connection.getDestination() & 0xFFFF] = null;
			m_connectionList.remove(connection);
			p_connection.cleanup();
			// TODO: Inform and update system
		}
		m_incomingOutgoingLock.unlock();
	}

}
