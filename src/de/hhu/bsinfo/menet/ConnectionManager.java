
package de.hhu.bsinfo.menet;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.net.events.ConnectionLostEvent;
import de.hhu.bsinfo.menet.AbstractConnection.DataReceiver;
import de.hhu.bsinfo.menet.AbstractConnectionCreator.ConnectionCreatorListener;

/**
 * Manages the network connections
 * @author Florian Klein 18.03.2012
 */
final class ConnectionManager implements ConnectionCreatorListener {

	// Constants
	protected static final int MAX_CONNECTIONS = 1000;

	// Attributes
	private AbstractConnection[] m_connections;
	private ArrayList<AbstractConnection> m_connectionList;

	private AbstractConnectionCreator m_creator;
	private ConnectionCreatorHelperThread m_connectionCreatorHelperThread;
	private DataReceiver m_connectionListener;

	private boolean m_deactivated;
	private boolean m_closed;

	private boolean m_waiting;
	private short m_waitingFor;

	private Condition m_connectionCreatedCondition;
	private ReentrantLock m_connectionCreationLock;
	private ReentrantLock m_getConnectionLock;

	// Constructors

	/**
	 * Creates an instance of ConnectionStore
	 * @param p_creator
	 *            the ConnectionCreator
	 * @param p_listener
	 *            the ConnectionListener
	 */
	ConnectionManager(final AbstractConnectionCreator p_creator, final DataReceiver p_listener) {
		m_connections = new AbstractConnection[65536];
		m_connectionList = new ArrayList<AbstractConnection>(MAX_CONNECTIONS);

		m_creator = p_creator;
		m_creator.setListener(this);
		m_connectionListener = p_listener;

		m_deactivated = false;
		m_closed = false;
		m_waiting = false;
		m_waitingFor = -1;

		m_connectionCreationLock = new ReentrantLock(false);
		m_connectionCreatedCondition = m_connectionCreationLock.newCondition();
		m_getConnectionLock = new ReentrantLock(false);

		// Start connection creator helper thread
		m_connectionCreatorHelperThread = new ConnectionCreatorHelperThread();
		m_connectionCreatorHelperThread.setName("Network: ConnectionCreatorHelperThread");
		m_connectionCreatorHelperThread.start();
	}

	/**
	 * Closes the ConnectionManager
	 */
	public void close() {
		m_closed = true;
		m_creator.close();
	}

	/**
	 * Returns the status of all connections
	 * @return the statuses
	 */
	protected String getConnectionStatuses() {
		String ret = "";

		m_connectionCreationLock.lock();
		Iterator<AbstractConnection> iter = m_connectionList.iterator();
		while (iter.hasNext()) {
			ret += iter.next().toString();
		}
		m_connectionCreationLock.unlock();

		return ret;
	}

	/**
	 * Checks if there is a congested connection
	 * @return whether there is congested connection or not
	 */
	protected boolean atLeastOneConnectionIsCongested() {
		boolean ret = false;

		m_connectionCreationLock.lock();
		Iterator<AbstractConnection> iter = m_connectionList.iterator();
		while (iter.hasNext()) {
			if (iter.next().isCongested()) {
				ret = true;
				break;
			}
		}
		m_connectionCreationLock.unlock();

		return ret;
	}

	/**
	 * Activates the connection manager
	 */
	protected void activate() {
		m_connectionCreationLock.lock();
		m_deactivated = false;
		m_connectionCreationLock.unlock();
	}

	/**
	 * Deactivates the connection manager
	 */
	protected void deactivate() {
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
	protected AbstractConnection getConnection(final short p_destination) throws IOException {
		AbstractConnection ret;

		assert p_destination != NodeID.INVALID_ID;

		ret = m_connections[p_destination & 0xFFFF];
		if (ret == null && !m_deactivated) {
			m_getConnectionLock.lock();
			m_connectionCreationLock.lock();

			ret = m_connections[p_destination & 0xFFFF];
			if (ret == null && !m_deactivated) {

				while (m_creator.keyIsPending() || m_waiting) {
					m_waiting = true;
					m_waitingFor = -1;
					try {
						// Wait for a connection to be finished or one ms if the pending key was closed
						m_connectionCreatedCondition.await(1, TimeUnit.MILLISECONDS);
					} catch (final InterruptedException e) {}
				}

				ret = m_connections[p_destination & 0xFFFF];
				if (ret == null && !m_deactivated) {
					/*-if (m_connectionList.size() == MAX_CONNECTIONS) {
						dismissRandomConnection();
					}*/

					try {
						ret = m_creator.createConnection(p_destination);
					} catch (final IOException e) {
						m_connectionCreationLock.unlock();
						m_getConnectionLock.unlock();

						throw e;
					}

					if (ret == null) {
						// This node's NodeID is smaller -> Remote node was triggered to create connection
						// Only one application thread can be in this section!
						m_waiting = true;
						m_waitingFor = p_destination;
						while (m_waiting) {
							try {
								m_connectionCreatedCondition.await();
							} catch (final InterruptedException e) {}
						}
						m_connectionCreationLock.unlock();
						m_getConnectionLock.unlock();

						return getConnection(p_destination);
					} else {
						// This node's NodeID is greater -> Keep established connection
						ret.setListener(m_connectionListener);
						addConnection(ret, false);
					}
				}
			}
			m_connectionCreationLock.unlock();
			m_getConnectionLock.unlock();
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

		// TODO: If maximum number of connections is reached, locally deleting connection does not impact remote node
		// TODO: Double connections problems
		// TODO:
		// TODO:
		// TODO:

		remoteNodeID = p_connection.getDestination();

		connection = m_connections[remoteNodeID & 0xFFFF];
		// if (connection == null) {
		if (m_connectionList.size() == MAX_CONNECTIONS) {
			dismissRandomConnection();
		}

		// No entry for this NodeID -> insert connection
		m_connections[remoteNodeID & 0xFFFF] = p_connection;
		m_connectionList.add(p_connection);
		/*-} else {
			// #if LOGGER >= ERROR
			NetworkHandler.getLogger().error(getClass().getSimpleName(),
					"Collision with already established connection to node " + NodeID.toHexString(remoteNodeID) + "!");
			// #endif /* LOGGER >= ERROR */
		// }
	}

	/**
	 * Dismiss the connection randomly
	 */
	private void dismissRandomConnection() {
		int random = -1;
		AbstractConnection dismiss = null;
		Random rand;

		rand = new Random();
		while (dismiss == null) {
			random = rand.nextInt(m_connections.length);
			dismiss = m_connections[random];
		}
		System.out.println("Removing " + NodeID.toHexString((short) random));
		m_connections[random] = null;
		m_connectionList.remove(dismiss);

		dismiss.close();
	}

	/**
	 * Closes the connection for the given destination
	 * @param p_destination
	 *            the destination
	 */
	/*-public void closeConnection(final short p_destination) {
		AbstractConnection connection;
	
		assert p_destination != NodeID.INVALID_ID;
	
		m_connectionCreationLock.lock();
		connection = m_connections[p_destination & 0xFFFF];
		m_connections[p_destination & 0xFFFF] = null;
		m_connectionList.remove(connection);
	
		if (connection != null) {
			connection.close();
		}
		m_connectionCreationLock.unlock();
	}*/

	/**
	 * A new connection must be created
	 * @param p_destination
	 *            the remote NodeID
	 * @note is called by selector thread only
	 */
	@Override
	public void createConnection(final short p_destination) {
		m_connectionCreatorHelperThread.pushJob(new Job((byte) 0, p_destination));
	}

	/**
	 * A new connection was created
	 * @param p_connection
	 *            the new connection
	 * @note is called by selector thread only
	 */
	@Override
	public void connectionCreated(final AbstractConnection p_connection) {
		p_connection.setListener(m_connectionListener);
		m_connectionCreatorHelperThread.pushJob(new Job((byte) 1, p_connection));
	}

	/**
	 * A connection was closed
	 * @param p_connection
	 *            the closed connection
	 * @note is called by selector thread only
	 */
	@Override
	public void connectionClosed(final AbstractConnection p_connection) {
		m_connectionCreatorHelperThread.pushJob(new Job((byte) 2, p_connection));
	}

	/**
	 * Helper thread that asynchronously executes commands for selector thread to avoid blocking it
	 * @author Kevin Beineke 22.06.2016
	 */
	private class ConnectionCreatorHelperThread extends Thread {

		private ArrayDeque<Job> m_jobs = new ArrayDeque<Job>();
		private ReentrantLock m_lock = new ReentrantLock(false);
		private Condition m_jobAvailableCondition = m_lock.newCondition();

		/**
		 * Push new job
		 * @param p_job
		 *            the new job to add
		 */
		private void pushJob(final Job p_job) {
			m_lock.lock();
			m_jobs.push(p_job);
			m_jobAvailableCondition.signal();
			m_lock.unlock();
		}

		@Override
		public void run() {
			short destination;
			AbstractConnection connection;
			Job job;

			while (!m_closed) {
				if (m_deactivated) {
					Thread.yield();
					continue;
				}

				m_lock.lock();
				while (m_jobs.isEmpty()) {
					try {
						m_jobAvailableCondition.await();
					} catch (final InterruptedException e) {}
				}

				job = m_jobs.pop();
				m_lock.unlock();

				if (job.getID() == 0) {
					// 0: Create and add connection
					destination = (short) job.getData();

					m_connectionCreationLock.lock();
					if (m_connections[destination & 0xFFFF] == null) {
						try {
							connection = m_creator.createConnection(destination);
							connection.setListener(m_connectionListener);

							addConnection(connection, false);
							if (m_waiting && (m_waitingFor == connection.getDestination() || m_waitingFor == -1)) {
								m_waiting = false;
								m_connectionCreatedCondition.signal();
							}
						} catch (final IOException e) { /* Ignore as this node does not know the failed node */}
					}
					m_connectionCreationLock.unlock();
				} else if (job.getID() == 1) {
					// 1: Connection was created -> Add it
					connection = (AbstractConnection) job.getData();

					m_connectionCreationLock.lock();
					addConnection(connection, true);
					if (m_waiting && (m_waitingFor == connection.getDestination() || m_waitingFor == -1)) {
						m_waiting = false;
						m_connectionCreatedCondition.signal();
					}
					m_connectionCreationLock.unlock();
				} else {
					// 2: Connection was closed by NIOSelectorThread (connection was faulty) -> Remove it
					connection = (AbstractConnection) job.getData();

					m_connectionCreationLock.lock();
					AbstractConnection tmp = m_connections[connection.getDestination() & 0xFFFF];
					if (tmp == connection) {
						m_connections[connection.getDestination() & 0xFFFF] = null;
						m_connectionList.remove(tmp);

						connection.cleanup();
						// TODO: Inform and update system
					}
					m_connectionCreationLock.unlock();

					// Trigger failure handling for remote node over faulty connection
					NetworkHandler.getEventHandler().fireEvent(
							new ConnectionLostEvent(getClass().getSimpleName(), connection.getDestination()));
				}
			}
		}
	}

	/**
	 * Helper class to encapsulate a job
	 * @author Kevin Beineke 22.06.2016
	 */
	private final class Job {
		private byte m_id;
		private Object m_data;

		/**
		 * Creates an instance of Job
		 * @param p_id
		 *            the static job identification
		 * @param p_data
		 *            the data (NodeID of destination or AbstractConnection depending on job)
		 */
		private Job(final byte p_id, final Object p_data) {
			m_id = p_id;
			m_data = p_data;
		}

		/**
		 * Returns the job identification
		 * @return the job ID
		 */
		public byte getID() {
			return m_id;
		}

		/**
		 * Returns the data
		 * @return the NodeID or AbstractConnection
		 */
		public Object getData() {
			return m_data;
		}
	}
}
