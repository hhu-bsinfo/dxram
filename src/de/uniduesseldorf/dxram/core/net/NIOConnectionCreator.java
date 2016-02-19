
package de.uniduesseldorf.dxram.core.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHelper;
import de.uniduesseldorf.dxram.core.io.InputHelper;
import de.uniduesseldorf.dxram.core.io.OutputHelper;
import de.uniduesseldorf.dxram.core.net.AbstractConnection.DataReceiver;
import de.uniduesseldorf.dxram.core.util.NodeID;

/**
 * Creates and manages new network connections using Java NIO
 * @author Florian Klein 18.03.2012
 *         Marc Ewert 11.08.2014
 */
class NIOConnectionCreator extends AbstractConnectionCreator {

	// Constants
	private static final int INCOMING_BUFFER_SIZE = 65536 * 2;
	private static final int OUTGOING_BUFFER_SIZE = 65536;
	private static final int SEND_BYTES = 1024 * 1024;
	private static final int CONNECTION_TIMEOUT = 200;

	private static final int MAX_OUTSTANDING_BYTES = BufferCache.MAX_MEMORY_CACHED;

	private static final boolean HIGH_PERFORMANCE = Core.getConfiguration().getBooleanValue(ConfigurationConstants.NETWORK_HIGH_PERFORMANCE);

	// Attributes
	private Worker m_worker;

	private NodesConfigurationHelper m_helper;

	private short m_nodeID;

	// Constructors
	/**
	 * Creates an instance of NIOConnectionCreator
	 */
	protected NIOConnectionCreator() {
		super();

		m_worker = null;

		m_helper = null;

		m_nodeID = NodeID.INVALID_ID;
	}

	// Methods
	/**
	 * Initializes the creator
	 */
	@Override
	public void initialize() {
		m_nodeID = NodeID.getLocalNodeID();

		m_helper = Core.getNodesConfiguration();

		m_worker = new Worker();
		m_worker.setName("Network: Selector");
		m_worker.init();
		m_worker.start();
	}

	/**
	 * Closes the creator and frees unused resources
	 */
	@Override
	public void close() {
		m_worker.close();
		m_worker = null;
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
		ret = new NIOConnection(p_destination, p_listener, condLock, cond);

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

	// Classes
	/**
	 * Represents a network connection
	 * @author Florian Klein 18.03.2012
	 */
	public class NIOConnection extends AbstractConnection {

		// Attributes
		private SocketChannel m_channel;

		private SelectionKeyHandler m_handler;

		private final Queue<ByteBuffer> m_incoming;
		private final ArrayDeque<ByteBuffer> m_outgoing;

		private int m_unconfirmedBytes;
		private int m_receivedBytes;

		private ReentrantLock m_connectionCondLock;
		private Condition m_connectionCond;

		private ReentrantLock m_flowControlCondLock;
		private Condition m_flowControlCond;

		private ReentrantLock m_incomingLock;
		private ReentrantLock m_outgoingLock;

		// Constructors
		/**
		 * Creates an instance of NIOConnection
		 * @param p_destination
		 *            the destination
		 * @param p_listener
		 *            the ConnectionListener
		 * @param p_lock
		 *            the ReentrantLock
		 * @param p_cond
		 *            the Condition
		 * @throws IOException
		 *             if the connection could not be created
		 */
		protected NIOConnection(final short p_destination, final DataReceiver p_listener,
				final ReentrantLock p_lock, final Condition p_cond) throws IOException {
			super(p_destination, p_listener);

			m_channel = SocketChannel.open();
			m_channel.configureBlocking(false);
			m_channel.socket().setSoTimeout(0);
			m_channel.socket().setTcpNoDelay(true);
			m_channel.socket().setSendBufferSize(OUTGOING_BUFFER_SIZE);
			m_channel.socket().setReceiveBufferSize(INCOMING_BUFFER_SIZE);

			m_worker.addOperationChangeRequest(new ChangeOperationsRequest(this, SelectionKey.OP_CONNECT));

			m_channel.connect(new InetSocketAddress(m_helper.getHost(p_destination), m_helper.getPort(p_destination)));

			m_incoming = new ArrayDeque<>();
			m_outgoing = new ArrayDeque<>();

			m_connectionCondLock = p_lock;
			m_connectionCond = p_cond;

			m_flowControlCondLock = new ReentrantLock(false);
			m_flowControlCond = m_flowControlCondLock.newCondition();
			m_incomingLock = new ReentrantLock(false);
			m_outgoingLock = new ReentrantLock(false);
		}

		/**
		 * Creates an instance of NIOConnection
		 * @param p_destination
		 *            the destination
		 * @param p_channel
		 *            the SocketChannel
		 * @throws IOException
		 *             if the connection could not be created
		 */
		protected NIOConnection(final short p_destination, final SocketChannel p_channel) throws IOException {
			super(p_destination);

			m_channel = p_channel;
			m_channel.configureBlocking(false);
			m_channel.socket().setSoTimeout(0);
			m_channel.socket().setTcpNoDelay(true);
			m_channel.socket().setSendBufferSize(OUTGOING_BUFFER_SIZE);
			m_channel.socket().setReceiveBufferSize(INCOMING_BUFFER_SIZE);

			m_incoming = new ArrayDeque<>();
			m_outgoing = new ArrayDeque<>();

			m_connectionCondLock = new ReentrantLock(false);
			m_connectionCond = m_connectionCondLock.newCondition();
			m_flowControlCondLock = new ReentrantLock(false);
			m_flowControlCond = m_flowControlCondLock.newCondition();
			m_incomingLock = new ReentrantLock(false);
			m_outgoingLock = new ReentrantLock(false);
		}

		// Methods
		/**
		 * Reads messages from the connection
		 * @return a AbstractMessage which was received
		 * @throws IOException
		 *             if the message could not be read
		 */
		@Override
		protected ByteBuffer doRead() {
			ByteBuffer ret;

			m_incomingLock.lock();
			ret = m_incoming.poll();
			m_incomingLock.unlock();

			m_receivedBytes += ret.remaining();

			m_flowControlCondLock.lock();
			if (m_receivedBytes > MAX_OUTSTANDING_BYTES / 8) {
				sendFlowControlMessage();
			}
			m_flowControlCondLock.unlock();

			return ret;
		}

		/**
		 * Append an incoming ByteBuffer to the Queue
		 * @param p_buffer
		 *            the ByteBuffer
		 */
		protected void addIncoming(final ByteBuffer p_buffer) {
			// Avoid congestion by not allowing more than 1000 buffers to be cached for reading
			while (m_incoming.size() > 1000) {//
				Thread.yield();
			}
			m_incomingLock.lock();
			m_incoming.offer(p_buffer);
			m_incomingLock.unlock();

			newData();
		}

		/**
		 * Writes data to the connection
		 * @param p_message
		 *            the AbstractMessage to send
		 * @throws IOException
		 *             if the data could not be written
		 */
		@Override
		protected void doWrite(final AbstractMessage p_message) {
			ByteBuffer buffer;

			buffer = p_message.getBuffer();
			m_flowControlCondLock.lock();
			while (m_unconfirmedBytes > MAX_OUTSTANDING_BYTES) {
				try {
					m_flowControlCond.await();
				} catch (final InterruptedException e) { /* ignore */}
			}

			m_unconfirmedBytes += buffer.remaining();
			m_flowControlCondLock.unlock();

			writeToChannel(buffer);
		}

		/**
		 * Enqueue buffer to be written into the channel
		 * @param p_buffer
		 *            Buffer
		 */
		private void writeToChannel(final ByteBuffer p_buffer) {
			// Avoid congestion by not allowing more than 1000 messages to be buffered for writing
			while (m_outgoing.size() > 1000) {
				Thread.yield();
			}

			m_outgoingLock.lock();
			// Change operation (read <-> write) and/or connection
			m_worker.addOperationChangeRequest(new ChangeOperationsRequest(this, SelectionKey.OP_WRITE)); // SelectionKey.OP_READ
			// |

			m_outgoing.offer(p_buffer);
			m_outgoingLock.unlock();
		}

		/**
		 * Prepend buffer to be written into the channel. Called if buffer could not be written completely.
		 * @param p_buffer
		 *            Buffer
		 */
		private void addBuffer(final ByteBuffer p_buffer) {
			m_outgoingLock.lock();
			// Change operation request to OP_READ to read before trying to send the buffer again
			m_worker.addOperationChangeRequest(new ChangeOperationsRequest(this, SelectionKey.OP_READ)); // |
			// SelectionKey.OP_WRITE

			m_outgoing.addFirst(p_buffer);
			m_outgoingLock.unlock();
		}

		/**
		 * Get the next buffers to be sent
		 * @param p_buffer
		 *            buffer to gather in data
		 * @param p_bytes
		 *            number of bytes to be sent
		 * @return Buffer array
		 */
		protected ByteBuffer getOutgoingBytes(final ByteBuffer p_buffer, final int p_bytes) {
			ByteBuffer buffer;
			ByteBuffer ret = null;
			int length = 0;

			while (!m_outgoing.isEmpty()) {
				m_outgoingLock.lock();
				buffer = m_outgoing.poll();
				m_outgoingLock.unlock();

				// This is a left-over (see addBuffer())
				if (buffer.remaining() != 0 && buffer.position() != 0) {
					return buffer;
				}

				// Skip when buffer is completed
				if (buffer == null || !buffer.hasRemaining()) {
					continue;
				}

				// Append when limit will not be reached
				if (length + buffer.remaining() <= p_bytes) {
					length += buffer.remaining();

					if (ret == null) {
						p_buffer.clear();
						ret = p_buffer;
					}
					ret.put(buffer);
				} else {
					// Append when limit has exceeded but buffer is still empty
					if (ret == null) {
						length += buffer.remaining();
						ret = buffer;
					} else {
						// Write back buffer for next sending call
						m_outgoingLock.lock();
						m_outgoing.addFirst(buffer);
						m_outgoingLock.unlock();
					}
					break;
				}
			}

			if (ret != null) {
				ret.position(0);
				if (length < ret.capacity()) {
					ret.limit(length);
				}
			}

			return ret;
		}

		/**
		 * Closes the connection
		 */
		@Override
		protected void doClose() {
			CloseConnectionRequest request;

			request = new CloseConnectionRequest(this);
			m_worker.addCloseConnectionRequest(request);
		}

		/**
		 * Executes after the connection is established
		 * @throws IOException
		 *             if the connection could not be accessed
		 */
		protected void connected() throws IOException {
			ByteBuffer temp;

			m_connectionCondLock.lock();
			try {
				m_channel.socket().setSendBufferSize(OUTGOING_BUFFER_SIZE);
				m_channel.socket().setReceiveBufferSize(INCOMING_BUFFER_SIZE);
			} catch (final IOException e) {
				m_connectionCondLock.unlock();
				throw e;
			}

			temp = ByteBuffer.allocate(2);
			OutputHelper.writeNodeID(temp, m_nodeID);
			temp.flip();

			writeToChannel(temp);

			setConnected(true);

			m_connectionCond.signalAll();
			m_connectionCondLock.unlock();
		}

		@Override
		protected void deliverMessage(final AbstractMessage p_message) {
			if (p_message instanceof NIOConnectionCreator.FlowControlMessage) {
				handleFlowControlMessage((FlowControlMessage) p_message);
			} else {
				super.deliverMessage(p_message);
			}
		}

		/**
		 * Confirm received bytes for the other node
		 */
		private void sendFlowControlMessage() {
			FlowControlMessage message;
			ByteBuffer messageBuffer;

			message = new FlowControlMessage(m_receivedBytes);
			messageBuffer = message.getBuffer();

			// add sending bytes for consistency
			m_unconfirmedBytes += messageBuffer.remaining();

			writeToChannel(messageBuffer);

			// reset received bytes counter
			m_receivedBytes = 0;
		}

		/**
		 * Handles a received FlowControlMessage
		 * @param p_message
		 *            FlowControlMessage
		 */
		private void handleFlowControlMessage(final FlowControlMessage p_message) {
			m_flowControlCondLock.lock();
			m_unconfirmedBytes -= p_message.getConfirmedBytes();

			m_flowControlCond.signalAll();
			m_flowControlCondLock.unlock();
		}
	}

	/**
	 * Manages the network connections
	 * @author Florian Klein 18.03.2012
	 */
	private class Worker extends Thread {

		// Attributes
		private ServerSocketChannel m_channel;
		private Selector m_selector;

		private final Queue<ChangeOperationsRequest> m_changeRequests;
		private final Queue<CloseConnectionRequest> m_closeRequests;

		private ReentrantLock m_changeLock;
		private ReentrantLock m_closeLock;

		private boolean m_running;

		// Constructors
		/**
		 * Creates an instance of Worker
		 */
		protected Worker() {
			m_channel = null;
			m_selector = null;

			m_changeRequests = new ArrayDeque<>();
			m_closeRequests = new ArrayDeque<>();

			m_running = false;

			m_changeLock = new ReentrantLock(false);
			m_closeLock = new ReentrantLock(false);
		}

		public void init() {
			IOException exception = null;

			// Create Selector on ServerSocketChannel
			for (int i = 0; i < 10; i++) {
				try {
					m_selector = Selector.open();
					m_channel = ServerSocketChannel.open();
					m_channel.configureBlocking(false);
					m_channel.socket().bind(new InetSocketAddress(Core.getConfiguration().getIntValue(ConfigurationConstants.NETWORK_PORT)));
					m_channel.register(m_selector, SelectionKey.OP_ACCEPT);

					m_running = true;

					exception = null;
					break;
				} catch (final IOException e) {
					exception = e;

					System.out.println("Could not bind network address. Retry in 1s.");

					try {
						Thread.sleep(1000);
					} catch (final InterruptedException e1) {}
				}
			}

			if (exception != null) {
				LOGGER.fatal("FATAL::Could not create network channel", exception);
			}
		}

		// Methods
		@Override
		public void run() {
			ChangeOperationsRequest changeRequest;
			Iterator<SelectionKey> iterator;
			Set<SelectionKey> selected;
			SelectionKey key;

			while (m_running) {
				// Handle pending ChangeOperationsRequests
				m_changeLock.lock();
				while (!m_changeRequests.isEmpty()) {
					changeRequest = m_changeRequests.poll();
					changeOptions(changeRequest.m_connection, changeRequest.m_operations);
				}
				m_changeLock.unlock();

				// Handle pending CloseConnectionRequests
				m_closeLock.lock();
				while (!m_closeRequests.isEmpty()) {
					closeConnection(m_closeRequests.poll().m_connection);
				}
				m_closeLock.unlock();

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
					LOGGER.error("ERROR::Accessing the selector", e);
				}
			}
		}

		/**
		 * Dispatch SelectionKey events
		 * @param p_key
		 *            SelectionKey to handle
		 */
		private void dispatch(final SelectionKey p_key) {
			NIOConnection connection;

			connection = (NIOConnection) p_key.attachment();
			if (connection != null) {
				if (connection.m_handler == null) {
					connection.m_handler = new SelectionKeyHandler();
				}

				connection.m_handler.execute(p_key, connection);
			} else {
				// handle unknown events by itself
				new SelectionKeyHandler().execute(p_key, null);
			}
		}

		/**
		 * Accept a new incoming connection
		 * @throws IOException
		 *             if the new connection could not be accesses
		 */
		private void accept() throws IOException {
			SocketChannel channel;

			channel = m_channel.accept();
			channel.configureBlocking(false);

			channel.register(m_selector, SelectionKey.OP_READ);
		}

		/**
		 * Append the given ChangeOperationsRequest to the Queue
		 * @param p_request
		 *            the ChangeOperationsRequest
		 */
		protected void addOperationChangeRequest(final ChangeOperationsRequest p_request) {
			m_changeLock.lock();
			m_changeRequests.offer(p_request);
			m_changeLock.unlock();

			m_selector.wakeup();
		}

		/**
		 * Append the given CloseConnectionRequest to the Queue
		 * @param p_request
		 *            the CloseConnectionRequest
		 */
		protected void addCloseConnectionRequest(final CloseConnectionRequest p_request) {
			m_closeLock.lock();
			m_closeRequests.offer(p_request);
			m_closeLock.unlock();

			m_selector.wakeup();
		}

		/**
		 * Changes the options for the given connection
		 * @param p_connection
		 *            the connection
		 * @param p_operations
		 *            the options
		 */
		private void changeOptions(final NIOConnection p_connection, final int p_operations) {
			try {
				p_connection.m_channel.register(m_selector, p_operations, p_connection);
			} catch (final ClosedChannelException e) {
				LOGGER.error("ERROR::Could not change operations");
			}
		}

		/**
		 * Closes the given connection
		 * @param p_connection
		 *            the connection
		 */
		private void closeConnection(final NIOConnection p_connection) {
			SelectionKey key;

			key = p_connection.m_channel.keyFor(m_selector);
			if (key != null) {
				key.cancel();
				try {
					p_connection.m_channel.close();
				} catch (final IOException e) {
					LOGGER.error("ERROR::Could not close connection to " + p_connection.getDestination(), e);
				}
				fireConnectionClosed(p_connection);
			}
		}

		/**
		 * Closes the Worker
		 */
		protected void close() {
			m_running = false;

			try {
				m_channel.close();
			} catch (final IOException e) {
				LOGGER.error("ERROR::Unable to close channel", e);
			}

			try {
				m_selector.close();
			} catch (final IOException e) {
				LOGGER.error("ERROR::Unable to close selector", e);
			}
		}
	}

	/**
	 * Represents a request to change the connection options
	 * @author Florian Klein 18.03.2012
	 */
	private class ChangeOperationsRequest {

		// Attributes
		private NIOConnection m_connection;
		private int m_operations;

		// Constructors
		/**
		 * Creates an instance of ChangeOperationsRequest
		 * @param p_connection
		 *            the connection
		 * @param p_operations
		 *            the operations
		 */
		protected ChangeOperationsRequest(final NIOConnection p_connection, final int p_operations) {
			m_connection = p_connection;
			m_operations = p_operations;
		}

	}

	/**
	 * Represents a request to close the connection
	 * @author Florian Klein 18.03.2012
	 */
	private class CloseConnectionRequest {

		// Attributes
		private NIOConnection m_connection;

		// Constructors
		/**
		 * Creates an instance of CloseConnectionRequest
		 * @param p_connection
		 *            the connection
		 */
		protected CloseConnectionRequest(final NIOConnection p_connection) {
			m_connection = p_connection;
		}

	}

	/**
	 * Wrapper class to handle SelectionKeys asynchronously
	 * @author Marc Ewert 03.09.2014
	 */
	private class SelectionKeyHandler {

		private final ByteBuffer m_readBuffer;
		private final ByteBuffer m_writeBuffer;

		private ReentrantLock m_connectionLock;
		private ReentrantLock m_bufferLock;
		private ReentrantLock m_writeLock;

		/**
		 * Creates a new SelectionKeyHandler
		 */
		SelectionKeyHandler() {
			m_readBuffer = ByteBuffer.allocateDirect(SEND_BYTES);
			m_writeBuffer = ByteBuffer.allocateDirect(SEND_BYTES);
			m_connectionLock = new ReentrantLock(false);
			m_bufferLock = new ReentrantLock(false);
			m_writeLock = new ReentrantLock(false);
		}

		/**
		 * Execute key by creating a new connection, reading from channel or writing to channel
		 * @param p_key
		 *            the current key
		 * @param p_connection
		 *            the connection
		 */
		public void execute(final SelectionKey p_key, final NIOConnection p_connection) {
			if (p_key.isValid()) {
				try {
					if (p_key.isReadable()) {
						if (p_connection == null) {
							createConnection((SocketChannel) p_key.channel());
						} else {
							read(p_connection);
						}
					} else if (p_key.isWritable()) {
						write(p_connection);
						p_key.interestOps(SelectionKey.OP_READ);
					} else if (p_key.isConnectable()) {
						connect(p_connection);
					}
				} catch (final IOException e) {
					LOGGER.debug("WARN::Accessing the channel");
				}
			}
		}

		/**
		 * Creates a new connection
		 * m_buffer needs to be synchronized externally
		 * @param p_channel
		 *            the channel of the connection
		 * @throws IOException
		 *             if the connection could not be created
		 */
		private void createConnection(final SocketChannel p_channel) throws IOException {
			NIOConnection connection;
			ByteBuffer buffer;

			m_connectionLock.lock();
			try {
				m_readBuffer.clear();

				if (p_channel.read(m_readBuffer) == -1) {
					p_channel.keyFor(m_worker.m_selector).cancel();
					p_channel.close();
				} else {
					m_readBuffer.flip();

					connection = new NIOConnection(InputHelper.readNodeID(m_readBuffer), p_channel);
					p_channel.register(m_worker.m_selector, SelectionKey.OP_READ, connection);

					fireConnectionCreated(connection);

					if (m_readBuffer.hasRemaining()) {
						buffer = ByteBuffer.allocate(m_readBuffer.remaining());
						buffer.put(m_readBuffer);
						buffer.flip();

						connection.addIncoming(buffer);
					}
				}
			} catch (final IOException e) {
				LOGGER.error("ERROR::Could not create connection", e);
				m_connectionLock.unlock();
				throw e;
			}
			m_connectionLock.unlock();
		}

		/**
		 * Reads from the given connection
		 * m_buffer needs to be synchronized externally
		 * @param p_connection
		 *            the Connection
		 * @throws IOException
		 *             if the data could not be read
		 */
		private void read(final NIOConnection p_connection) throws IOException {
			ByteBuffer buffer;
			long readBytes = 0;

			m_bufferLock.lock();
			try {
				m_readBuffer.clear();
				while (m_readBuffer.position() + INCOMING_BUFFER_SIZE <= m_readBuffer.capacity()) {
					m_readBuffer.limit(m_readBuffer.position() + INCOMING_BUFFER_SIZE);

					try {
						readBytes = p_connection.m_channel.read(m_readBuffer);
					} catch (final ClosedChannelException e) {
						// Channel is closed -> ignore
						break;
					}
					if (readBytes == -1) {
						m_worker.closeConnection(p_connection);
						break;
					} else if (readBytes == 0 && m_readBuffer.position() != 0) {
						m_readBuffer.limit(m_readBuffer.position());
						m_readBuffer.position(0);

						buffer = ByteBuffer.allocate(m_readBuffer.limit());
						buffer.put(m_readBuffer);
						buffer.flip();
						p_connection.addIncoming(buffer);
						break;
					}
				}
			} catch (final IOException e) {
				LOGGER.debug("WARN::Could not read from channel (" + p_connection.getDestination() + ")", e);
				m_bufferLock.unlock();
				throw e;
			}
			m_bufferLock.unlock();
		}

		/**
		 * Writes to the given connection
		 * @param p_connection
		 *            the connection
		 * @throws IOException
		 *             if the data could not be written
		 */
		public void write(final NIOConnection p_connection) throws IOException {
			int writtenBytes = 0;
			int length = 0;
			int bytes;
			int size;
			ByteBuffer view;
			ByteBuffer buffer;

			m_writeLock.lock();
			buffer = p_connection.getOutgoingBytes(m_writeBuffer, SEND_BYTES);
			try {
				if (buffer != null) {
					writtenBytes += buffer.remaining();
					if (buffer.remaining() > SEND_BYTES) {
						// The write-Method for NIO SocketChannels is very slow for large buffers to write regardless of
						// the length of the actual written data -> simulate a smaller buffer by slicing it
						outerloop: while (buffer.remaining() > 0) {
							size = Math.min(buffer.remaining(), SEND_BYTES);
							view = buffer.slice();
							view.limit(size);

							length = view.remaining();
							int tries = 0;
							while (length > 0) {
								try {
									bytes = p_connection.m_channel.write(view);
									length -= bytes;

									if (bytes == 0) {
										if (++tries == 1000000) {
											System.out.println("Cannot write buffer because receive buffer has not been read for a while.");
											buffer.position(buffer.position() + size - length);
											view = buffer.slice();
											p_connection.addBuffer(view);

											break outerloop;
										}
									} else {
										tries = 0;
									}
								} catch (final ClosedChannelException e) {
									// Channel is closed -> ignore
									break;
								}
							}
							buffer.position(buffer.position() + size);
						}
					} else {
						length = buffer.remaining();
						int tries = 0;
						while (length > 0) {
							try {
								bytes = p_connection.m_channel.write(buffer);
								length -= bytes;

								if (bytes == 0) {
									if (++tries == 1000000) {
										System.out.println("Cannot write buffer because receive buffer has not been read for a while.");
										p_connection.addBuffer(buffer);
										break;
									}
								} else {
									tries = 0;
								}
							} catch (final ClosedChannelException e) {
								// Channel is closed -> ignore
								break;
							}
						}
					}
					ThroughputStatistic.getInstance().outgoingExtern(writtenBytes - length);
				}
			} catch (final IOException e) {
				LOGGER.debug("WARN::Could not write to channel (" + p_connection.getDestination() + ")", e);
				m_writeLock.unlock();
				throw e;
			}
			m_writeLock.unlock();
		}

		/**
		 * Finishes the connection process for the given connection
		 * @param p_connection
		 *            the connection
		 * @throws IOException
		 *             if connection could not be finalized
		 */
		private void connect(final NIOConnection p_connection) {
			m_connectionLock.lock();
			try {
				if (p_connection.m_channel.isConnectionPending()) {
					p_connection.m_channel.finishConnect();
					p_connection.connected();
				}
			} catch (final IOException e) {/* ignore */}
			m_connectionLock.unlock();
		}

	}

	/**
	 * Used to confirm received bytes
	 * @author Marc Ewert 14.10.2014
	 */
	public static final class FlowControlMessage extends AbstractMessage {

		public static final byte TYPE = 0;
		public static final byte SUBTYPE = 1;

		private int m_confirmedBytes;

		/**
		 * Default constructor for serialization
		 */
		FlowControlMessage() {}

		/**
		 * Create a new Message for confirming received bytes.
		 * @param p_confirmedBytes
		 *            number of received bytes
		 */
		FlowControlMessage(final int p_confirmedBytes) {
			super((short) 0, TYPE, SUBTYPE, true);
			m_confirmedBytes = p_confirmedBytes;
		}

		/**
		 * Get number of confirmed bytes
		 * @return
		 *         the number of confirmed bytes
		 */
		public int getConfirmedBytes() {
			return m_confirmedBytes;
		}

		@Override
		protected void readPayload(final ByteBuffer p_buffer) {
			m_confirmedBytes = p_buffer.getInt();
		}

		@Override
		protected void writePayload(final ByteBuffer p_buffer) {
			p_buffer.putInt(m_confirmedBytes);
		}

		@Override
		protected int getPayloadLength() {
			return 4;
		}
	}
}
