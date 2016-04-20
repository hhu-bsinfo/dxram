
package de.hhu.bsinfo.menet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages the network connections
 * @author Florian Klein 18.03.2012
 */
class NIOSelector extends Thread {

	// Attributes
	private ServerSocketChannel m_serverChannel;
	private Selector m_selector;

	private NIOConnectionCreator m_connectionCreator;
	private NIOInterface m_nioInterface;

	private final LinkedHashSet<ChangeOperationsRequest> m_changeRequests;
	private ReentrantLock m_changeLock;

	private boolean m_running;

	// Constructors
	/**
	 * Creates an instance of NIOSelector
	 * @param p_connectionCreator
	 *            the NIOConnectionCreator
	 * @param p_nioInterface
	 *            the NIOInterface to send/receive data
	 * @param p_port
	 *            the port
	 */
	protected NIOSelector(final NIOConnectionCreator p_connectionCreator, final NIOInterface p_nioInterface, final int p_port) {
		m_serverChannel = null;
		m_selector = null;

		m_nioInterface = p_nioInterface;
		m_connectionCreator = p_connectionCreator;

		m_changeRequests = new LinkedHashSet<ChangeOperationsRequest>();
		m_changeLock = new ReentrantLock(false);

		m_running = false;

		// Create Selector on ServerSocketChannel
		IOException exception = null;
		for (int i = 0; i < 10; i++) {
			try {
				m_selector = Selector.open();
				m_serverChannel = ServerSocketChannel.open();
				m_serverChannel.configureBlocking(false);
				m_serverChannel.socket().bind(new InetSocketAddress(p_port));
				m_serverChannel.register(m_selector, SelectionKey.OP_ACCEPT);

				m_running = true;

				exception = null;
				break;
			} catch (final IOException e) {
				exception = e;

				NetworkHandler.getLogger().error(getClass().getSimpleName(), "Could not bind network address. Retry in 1s.");

				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e1) {}
			}
		}

		if (exception != null) {
			NetworkHandler.getLogger().error(getClass().getSimpleName(), "Could not create network channel!");
		}
	}

	// Getter
	/**
	 * Returns the Selector
	 * @return the Selector
	 */
	protected Selector getSelector() {
		return m_selector;
	}

	// Methods
	@Override
	public String toString() {
		String ret = "Current keys: ";

		Set<SelectionKey> selected = m_selector.keys();
		Iterator<SelectionKey> iterator = selected.iterator();
		while (iterator.hasNext()) {
			SelectionKey key = iterator.next();
			if (key.attachment() != null) {
				ret += "[" + ((NIOConnection) key.attachment()).getDestination() + ", " + key.interestOps() + "] ";
			}
		}

		return ret;
	}

	@Override
	public void run() {
		int interest;
		NIOConnection connection;
		ChangeOperationsRequest changeRequest;
		Iterator<SelectionKey> iterator;
		Set<SelectionKey> selected;
		SelectionKey key;

		while (m_running) {
			m_changeLock.lock();
			Iterator<ChangeOperationsRequest> iter;
			while (!m_changeRequests.isEmpty()) {
				iter = m_changeRequests.iterator();
				changeRequest = iter.next();
				iter.remove();

				connection = changeRequest.getConnection();
				interest = changeRequest.getOperations();
				if (interest != -1) {
					try {
						key = connection.getChannel().register(m_selector, interest, connection);
					} catch (final ClosedChannelException e) {
						NetworkHandler.getLogger().error(getClass().getSimpleName(), "Could not change operations!");
					}
				} else {
					m_connectionCreator.closeConnection(connection);
				}
			}
			m_changeLock.unlock();

			try {
				// Wait for network action
				if (m_selector.select() > 0 && m_selector.isOpen()) {
					selected = m_selector.selectedKeys();
					iterator = selected.iterator();

					while (iterator.hasNext()) {
						key = iterator.next();
						iterator.remove();
						if (key.isValid()) {
							if (key.isAcceptable()) {
								accept();
							} else {
								dispatch(key);
							}
						}
					}
					selected.clear();
				}
			} catch (final Exception e) {
				NetworkHandler.getLogger().error(getClass().getSimpleName(), "Key selection failed!");
			}
		}
	}

	/**
	 * Accept a new incoming connection
	 * @throws IOException
	 *             if the new connection could not be accesses
	 */
	private void accept() throws IOException {
		SocketChannel channel;

		channel = m_serverChannel.accept();
		channel.configureBlocking(false);

		channel.register(m_selector, SelectionKey.OP_READ);
	}

	/**
	 * Execute key by creating a new connection, reading from channel or writing to channel
	 * @param p_key
	 *            the current key
	 */
	public void dispatch(final SelectionKey p_key) {
		boolean complete = true;
		boolean successful = true;
		NIOConnection connection;

		connection = (NIOConnection) p_key.attachment();
		if (p_key.isValid()) {
			try {
				if (p_key.isReadable()) {
					if (connection == null) {
						m_connectionCreator.createConnection((SocketChannel) p_key.channel());
					} else {
						try {
							successful = m_nioInterface.read(connection);
						} catch (final IOException e) {
							NetworkHandler.getLogger().error(getClass().getSimpleName(),
									"Could not read from channel (" + NodeID.toHexString(connection.getDestination()) + ")!");
							successful = false;
						}
						if (!successful) {
							m_connectionCreator.closeConnection(connection);
						}
					}
				} else if (p_key.isWritable()) {
					try {
						complete = m_nioInterface.write(connection);
					} catch (final IOException e) {
						NetworkHandler.getLogger().error(getClass().getSimpleName(),
								"Could not write to channel (" + NodeID.toHexString(connection.getDestination()) + ")!");
						complete = false;
					}

					if (!complete) {
						// If there is still data left to write on this connection, add another write request
						m_changeLock.lock();
						m_changeRequests.add(new ChangeOperationsRequest(connection, SelectionKey.OP_WRITE));
						m_changeLock.unlock();
					}
					// Set interest to READ after writing; do not if channel was blocked and data is left
					p_key.interestOps(SelectionKey.OP_READ);
				} else if (p_key.isConnectable()) {
					NIOInterface.connect(connection);
				}
			} catch (final IOException e) {
				NetworkHandler.getLogger().error(getClass().getSimpleName(), "Could not access channel properly!");
			}
		}
	}

	/**
	 * Append the given ChangeOperationsRequest to the Queue
	 * @param p_request
	 *            the ChangeOperationsRequest
	 */
	protected void changeOperationInterestAsync(final ChangeOperationsRequest p_request) {
		m_changeLock.lock();
		m_changeRequests.add(p_request);
		m_changeLock.unlock();

		m_selector.wakeup();
	}

	/**
	 * Append the given NIOConnection to the Queue
	 * @param p_connection
	 *            the NIOConnection to close
	 */
	protected void closeConnectionAsync(final NIOConnection p_connection) {
		m_changeLock.lock();
		m_changeRequests.add(new ChangeOperationsRequest(p_connection, -1));
		m_changeLock.unlock();

		m_selector.wakeup();
	}

	/**
	 * Closes the Worker
	 */
	protected void close() {
		m_running = false;

		try {
			m_serverChannel.close();
		} catch (final IOException e) {
			NetworkHandler.getLogger().error(getClass().getSimpleName(), "Unable to close channel!");
		}
		NetworkHandler.getLogger().info(getClass().getSimpleName(), "Closing ServerSocketChannel successful.");

		try {
			m_selector.close();
		} catch (final IOException e) {
			NetworkHandler.getLogger().error(getClass().getSimpleName(), "Unable to shutdown selector!");
		}
		NetworkHandler.getLogger().info(getClass().getSimpleName(), "Shutdown of Selector successful.");
	}
}
