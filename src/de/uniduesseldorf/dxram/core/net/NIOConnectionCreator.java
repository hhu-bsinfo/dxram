
package de.uniduesseldorf.dxram.core.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.net.AbstractConnection.DataReceiver;

/**
 * Creates and manages new network connections using Java NIO
 * @author Florian Klein 18.03.2012
 *         Marc Ewert 11.08.2014
 */
class NIOConnectionCreator extends AbstractConnectionCreator {

	// Constants
	protected static final int INCOMING_BUFFER_SIZE = (int) (512 * 1024 * 1.2); // 1024 * 1024 * 2;// 65536 * 2;
	protected static final int OUTGOING_BUFFER_SIZE = 512 * 1024; // 1024 * 1024;// 65536;
	protected static final int RECEIVE_BYTES = (int) (512 * 1024 * 1.2); // 1024 * 1024;
	protected static final int SEND_BYTES = 512 * 1024; // 1024 * 1024;
	protected static final int CONNECTION_TIMEOUT = 200;

	// Attributes
	private NIOSelector m_nioSelector;

	// Constructors
	/**
	 * Creates an instance of NIOConnectionCreator
	 */
	protected NIOConnectionCreator() {
		super();

		m_nioSelector = null;
	}

	// Methods
	/**
	 * Initializes the creator
	 */
	@Override
	public void initialize() {
		m_nioSelector = new NIOSelector(this);
		m_nioSelector.setName("Network: NIOSelector");
		m_nioSelector.setPriority(Thread.MAX_PRIORITY);
		m_nioSelector.start();
	}

	/**
	 * Closes the creator and frees unused resources
	 */
	@Override
	public void close() {
		m_nioSelector.close();
		m_nioSelector = null;
	}

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
	@Override
	public NIOConnection createConnection(final short p_destination, final DataReceiver p_listener) throws IOException {
		NIOConnection ret;
		ReentrantLock condLock;
		Condition cond;
		long timeStart;
		long timeNow;

		condLock = new ReentrantLock(false);
		cond = condLock.newCondition();
		ret = new NIOConnection(p_destination, p_listener, condLock, cond, m_nioSelector);

		timeStart = System.currentTimeMillis();
		condLock.lock();
		while (!ret.isConnected()) {
			timeNow = System.currentTimeMillis();
			if (timeNow - timeStart > CONNECTION_TIMEOUT) {
				LOGGER.debug("connection time-out");

				condLock.unlock();
				throw new IOException("Timeout occurred");
			}
			try {
				cond.awaitNanos(20000);
			} catch (final InterruptedException e) { /* ignore */}
		}
		condLock.unlock();

		return ret;
	}

	/**
	 * Creates a new connection, triggered by incoming key
	 * m_buffer needs to be synchronized externally
	 * @param p_channel
	 *            the channel of the connection
	 * @throws IOException
	 *             if the connection could not be created
	 */
	protected void createConnection(final SocketChannel p_channel) throws IOException {
		NIOConnection connection;

		try {
			connection = NIOInterface.initIncomingConnection(p_channel, m_nioSelector);
			fireConnectionCreated(connection);
		} catch (final IOException e) {
			System.out.println("ERROR::Could not create connection");
			throw e;
		}
	}

	/**
	 * Closes the given connection
	 * @param p_connection
	 *            the connection
	 */
	protected void closeConnection(final NIOConnection p_connection) {
		SelectionKey key;

		key = p_connection.getChannel().keyFor(m_nioSelector.getSelector());
		if (key != null) {
			key.cancel();
			try {
				p_connection.getChannel().close();
			} catch (final IOException e) {
				System.out.println("ERROR::Could not close connection to " + p_connection.getDestination());
			}
			fireConnectionClosed(p_connection);
		}
	}

}
