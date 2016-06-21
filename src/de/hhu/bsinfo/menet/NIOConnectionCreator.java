
package de.hhu.bsinfo.menet;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.menet.AbstractConnection.DataReceiver;

/**
 * Creates and manages new network connections using Java NIO
 * @author Florian Klein 18.03.2012
 *         Marc Ewert 11.08.2014
 */
class NIOConnectionCreator extends AbstractConnectionCreator {

	// Attributes
	private MessageCreator m_messageCreator;
	private NIOSelector m_nioSelector;

	private MessageDirectory m_messageDirectory;
	private NIOInterface m_nioInterface;
	private NodeMap m_nodeMap;

	private int m_incomingBufferSize;
	private int m_outgoingBufferSize;
	private int m_numberOfBuffers;
	private int m_flowControlWindowSize;
	private int m_connectionTimeout;

	// Constructors
	/**
	 * Creates an instance of NIOConnectionCreator
	 * @param p_messageDirectory
	 *            the message directory
	 * @param p_nodeMap
	 *            the node map
	 * @param p_incomingBufferSize
	 *            the size of incoming buffer
	 * @param p_outgoingBufferSize
	 *            the size of outgoing buffer
	 * @param p_numberOfBuffers
	 *            the number of bytes until a flow control message must be received to continue sending
	 * @param p_flowControlWindowSize
	 *            the maximal number of ByteBuffer to schedule for sending/receiving
	 * @param p_connectionTimeout
	 *            the connection timeout
	 */
	protected NIOConnectionCreator(final MessageDirectory p_messageDirectory,
			final NodeMap p_nodeMap, final int p_incomingBufferSize, final int p_outgoingBufferSize,
			final int p_numberOfBuffers, final int p_flowControlWindowSize, final int p_connectionTimeout) {
		super();

		m_nioSelector = null;

		m_messageDirectory = p_messageDirectory;

		m_incomingBufferSize = p_incomingBufferSize;
		m_outgoingBufferSize = p_outgoingBufferSize;
		m_numberOfBuffers = p_numberOfBuffers;
		m_flowControlWindowSize = p_flowControlWindowSize;
		m_connectionTimeout = p_connectionTimeout;

		m_nodeMap = p_nodeMap;

		m_nioInterface = new NIOInterface(p_incomingBufferSize, p_outgoingBufferSize, p_flowControlWindowSize);
	}

	// Methods
	/**
	 * Initializes the creator
	 */
	@Override
	public void initialize(final short p_nodeID, final int p_listenPort) {
		// #if LOGGER >= INFO
		NetworkHandler.getLogger().info(getClass().getSimpleName(), "Network: MessageCreator");
		// #endif /* LOGGER >= INFO */
		m_messageCreator = new MessageCreator(m_numberOfBuffers);
		m_messageCreator.setName("Network: MessageCreator");
		m_messageCreator.setPriority(Thread.MAX_PRIORITY);
		m_messageCreator.start();

		// #if LOGGER >= INFO
		NetworkHandler.getLogger().info(getClass().getSimpleName(), "Network: NIOSelector");
		// #endif /* LOGGER >= INFO */
		m_nioSelector = new NIOSelector(this, m_nioInterface, p_listenPort);
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

		m_messageCreator.shutdown();
		m_messageCreator = null;
	}

	@Override
	public String getSelectorStatus() {
		return m_nioSelector.toString();
	}

	@Override
	public boolean keyIsPending() {
		byte counter = 0;

		try {
			Iterator<SelectionKey> iter = m_nioSelector.getSelector().keys().iterator();
			while (iter.hasNext()) {
				if (iter.next().attachment() == null && ++counter == 2) {
					return true;
				}
			}
		} catch (final ConcurrentModificationException e) {
			// A connection was closed during iteration -> try again
			return keyIsPending();
		}

		return false;
	}

	/**
	 * Creates a new connection to the given destination
	 * @param p_destination
	 *            the destination
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
		ret = new NIOConnection(p_destination, m_nodeMap, m_messageDirectory, condLock, cond, m_messageCreator, m_nioSelector,
				m_numberOfBuffers, m_incomingBufferSize, m_outgoingBufferSize, m_flowControlWindowSize);

		ret.connect();

		timeStart = System.currentTimeMillis();
		condLock.lock();
		while (!ret.isConnected()) {
			timeNow = System.currentTimeMillis();
			if (timeNow - timeStart > m_connectionTimeout) {
				// #if LOGGER >= DEBUG
				NetworkHandler.getLogger().debug(getClass().getSimpleName(), "connection creation time-out. Interval "
						+ m_connectionTimeout + "ms might be to small");
				// #endif /* LOGGER >= DEBUG */

				condLock.unlock();
				throw new IOException("Timeout occurred");
			}
			try {
				cond.awaitNanos(1000);
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
			connection = m_nioInterface.initIncomingConnection(m_nodeMap, m_messageDirectory, p_channel, m_messageCreator, m_nioSelector, m_numberOfBuffers);
			if (connection != null) {
				fireConnectionCreated(connection);
			}
		} catch (final IOException e) {
			// #if LOGGER >= ERROR
			NetworkHandler.getLogger().error(getClass().getSimpleName(), "Could not create connection!");
			// #endif /* LOGGER >= ERROR */
			throw e;
		}
	}

	/**
	 * Closes the given connection
	 * @param p_connection
	 *            the connection
	 * @param p_informConnectionManager
	 *            whether to inform the connection manager or not
	 */
	protected void closeConnection(final NIOConnection p_connection, final boolean p_informConnectionManager) {
		SelectionKey key;

		key = p_connection.getChannel().keyFor(m_nioSelector.getSelector());
		if (key != null) {
			key.cancel();
			try {
				p_connection.getChannel().close();
			} catch (final IOException e) {
				// #if LOGGER >= ERROR
				NetworkHandler.getLogger().error(getClass().getSimpleName(), "Could not close connection to " + p_connection.getDestination() + "!");
				// #endif /* LOGGER >= ERROR */
			}
			if (p_informConnectionManager) {
				fireConnectionClosed(p_connection);
			}
		}
	}

}
