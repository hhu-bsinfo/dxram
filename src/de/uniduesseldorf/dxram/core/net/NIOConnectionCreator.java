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
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHelper;
import de.uniduesseldorf.dxram.core.io.InputHelper;
import de.uniduesseldorf.dxram.core.io.OutputHelper;
import de.uniduesseldorf.dxram.core.net.AbstractConnection.DataReceiver;

/**
 * Creates and manages new network connections using Java NIO
 * @author Florian Klein 18.03.2012
 *         Marc Ewert 11.08.2014
 */
class NIOConnectionCreator extends AbstractConnectionCreator {

	// Constants
	private static final int INCOMING_BUFFER_SIZE = 65536 * 2;
	private static final int OUTGOING_BUFFER_SIZE = 65536;
	private static final int SEND_BYTES = 1024;

	private static final int MAX_OUTSTANDING_BYTES = BufferCache.MAX_MEMORY_CACHED;

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
	public NIOConnection createConnection(final short p_destination, final DataReceiver p_listener)
			throws IOException {
		NIOConnection ret;
		long timeStart;
		long timeNow;

		ret = new NIOConnection(p_destination, p_listener);

		timeStart = System.currentTimeMillis();
		synchronized (ret) {
			while (!ret.isConnected()) {
				timeNow = System.currentTimeMillis();
				if (timeNow - timeStart > 500) {
					LOGGER.warn("connection time-out");

					throw new IOException("Timeout occurred");
				}
				try {
					ret.wait(20);
				} catch (final InterruptedException e) { /* ignore */}
			}
		}

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

		// Constructors
		/**
		 * Creates an instance of NIOConnection
		 * @param p_destination
		 *            the destination
		 * @param p_listener
		 *            the ConnectionListener
		 * @throws IOException
		 *             if the connection could not be created
		 */
		protected NIOConnection(final short p_destination, final DataReceiver p_listener) throws IOException {
			super(p_destination, p_listener);

			m_channel = SocketChannel.open();
			m_channel.configureBlocking(false);

			m_worker.addOperationChangeRequest(new ChangeOperationsRequest(this, SelectionKey.OP_CONNECT));

			m_channel
					.connect(new InetSocketAddress(m_helper.getHost(p_destination), m_helper.getPort(p_destination)));

			m_incoming = new ArrayDeque<>();
			m_outgoing = new ArrayDeque<>();
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
			m_channel.socket().setSendBufferSize(OUTGOING_BUFFER_SIZE);
			m_channel.socket().setReceiveBufferSize(INCOMING_BUFFER_SIZE);

			m_incoming = new ArrayDeque<>();
			m_outgoing = new ArrayDeque<>();
		}

		// Methods
		/**
		 * Reads messages from the connection
		 * @return a AbstractMessage which was received
		 * @throws IOException
		 *             if the message could not be read
		 */
		@Override
		protected ByteBuffer doRead() throws IOException {
			ByteBuffer ret;

			synchronized (m_incoming) {
				ret = m_incoming.poll();
			}

			m_receivedBytes += ret.remaining();

			synchronized (this) {
				if (m_receivedBytes > MAX_OUTSTANDING_BYTES / 8) {
					sendFlowControlMessage();
				}
			}

			return ret;
		}

		/**
		 * Append an incoming ByteBuffer to the Queue
		 * @param p_buffer
		 *            the ByteBuffer
		 */
		protected void addIncoming(final ByteBuffer p_buffer) {
			synchronized (m_incoming) {
				m_incoming.offer(p_buffer);
			}

			// fireNewData();
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
		protected void doWrite(final AbstractMessage p_message) throws IOException {
			final ByteBuffer buffer = p_message.getBuffer();

			synchronized (this) {
				while (m_unconfirmedBytes > MAX_OUTSTANDING_BYTES) {
					try {
						LOGGER.info(super.getDestination() + ": waiting " + m_unconfirmedBytes + " of "
								+ MAX_OUTSTANDING_BYTES);
						super.wait();
					} catch (final InterruptedException e) { /* ignore */}
				}

				m_unconfirmedBytes += buffer.remaining();
			}

			writeToChannel(buffer);
		}

		/**
		 * Enqueue buffer to be written into the channel
		 * @param p_buffer
		 *            Buffer
		 */
		private void writeToChannel(final ByteBuffer p_buffer) {
			synchronized (m_outgoing) {
				if (m_outgoing.isEmpty()) {
					m_worker.addOperationChangeRequest(new ChangeOperationsRequest(this, SelectionKey.OP_READ
							| SelectionKey.OP_WRITE));
				}

				m_outgoing.offer(p_buffer);
			}
		}

		/**
		 * Get the next buffers to be sent
		 * @param p_bytes
		 *            number of bytes to be sent
		 * @return Buffer array
		 */
		protected ByteBuffer[] getOutgoingBytes(final int p_bytes) {
			final LinkedList<ByteBuffer> buffers = new LinkedList<>();
			ByteBuffer buffer;
			ByteBuffer[] ret = null;
			int length = 0;

			while (!m_outgoing.isEmpty()) {
				synchronized (m_outgoing) {
					buffer = m_outgoing.poll();
				}

				// skip when buffer is completed
				if (buffer == null || !buffer.hasRemaining()) {
					continue;
				}

				// append when limit will not be reached
				if (length + buffer.remaining() <= p_bytes) {
					length += buffer.remaining();
					buffers.add(buffer);
				} else {
					// append when limit has exceeded but buffer is still empty
					if (buffers.isEmpty()) {
						length += buffer.remaining();
						buffers.add(buffer);
					} else {
						// write back buffer for next sending call
						synchronized (m_outgoing) {
							m_outgoing.addFirst(buffer);
						}
					}

					break;
				}
			}

			if (length > 0) {
				ret = buffers.toArray(new ByteBuffer[buffers.size()]);

				synchronized (m_outgoing) {
					for (int i = ret.length - 1;i >= 0;i--) {
						m_outgoing.addFirst(ret[i]);
					}
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
		protected synchronized void connected() throws IOException {
			ByteBuffer temp;

			m_channel.socket().setSendBufferSize(OUTGOING_BUFFER_SIZE);
			m_channel.socket().setReceiveBufferSize(INCOMING_BUFFER_SIZE);

			temp = ByteBuffer.allocate(2);
			OutputHelper.writeNodeID(temp, m_nodeID);
			temp.flip();

			writeToChannel(temp);

			setConnected(true);

			notifyAll();
		}

		@Override
		protected void deliverMessage(final AbstractMessage p_message) {
			if (p_message instanceof NIOConnectionCreator.FlowControlMessage) {
				handleFlowControlMessage((FlowControlMessage)p_message);
			} else {
				super.deliverMessage(p_message);
			}
		}

		/**
		 * Confirm received bytes for the other node
		 */
		private void sendFlowControlMessage() {
			final FlowControlMessage message = new FlowControlMessage(m_receivedBytes);
			final ByteBuffer messageBuffer = message.getBuffer();

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
		private synchronized void handleFlowControlMessage(final FlowControlMessage p_message) {
			m_unconfirmedBytes -= p_message.getConfirmedBytes();

			super.notifyAll();
		}
	}

	/**
	 * Manages the network connections
	 * @author Florian Klein 18.03.2012
	 */
	private class Worker implements Runnable {

		// Attributes
		private ServerSocketChannel m_channel;
		private Selector m_selector;

		private final TaskExecutor m_executor;

		private final Queue<ChangeOperationsRequest> m_changeRequests;
		private final Queue<CloseConnectionRequest> m_closeRequests;

		private boolean m_running;

		// Constructors
		/**
		 * Creates an instance of Worker
		 */
		protected Worker() {
			m_channel = null;
			m_selector = null;

			m_executor = new TaskExecutor("NIO", Core.getConfiguration().getIntValue(
					ConfigurationConstants.NETWORK_NIO_THREADCOUNT));

			m_changeRequests = new ArrayDeque<>();
			m_closeRequests = new ArrayDeque<>();

			m_running = false;
		}

		/**
		 * Starts the worker
		 */
		public void start() {
			IOException exception = null;

			// Create Selector an ServerSocketChannel
			for (int i = 0;i < 10;i++) {
				try {
					m_selector = Selector.open();
					m_channel = ServerSocketChannel.open();
					m_channel.configureBlocking(false);
					m_channel.socket().bind(
							new InetSocketAddress(Core.getConfiguration().getIntValue(
									ConfigurationConstants.NETWORK_PORT)));
					m_channel.register(m_selector, SelectionKey.OP_ACCEPT);

					m_running = true;

					m_executor.execute(this);

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
			final Selector selector = this.m_selector;

			try {
				// Handle pending ChangeOperationsRequests
				synchronized (m_changeRequests) {
					while (!m_changeRequests.isEmpty()) {
						changeRequest = m_changeRequests.poll();
						changeOptions(changeRequest.m_connection, changeRequest.m_operations);
					}
				}

				// Handle pending CloseConnectionRequests
				synchronized (m_closeRequests) {
					while (!m_closeRequests.isEmpty()) {
						closeConnection(m_closeRequests.poll().m_connection);
					}
				}

				// Wait for network action
				if (selector.select() > 0 && selector.isOpen()) {
					selected = selector.selectedKeys();
					iterator = selected.iterator();

					while (iterator.hasNext()) {
						final SelectionKey key = iterator.next();
						if (key.isValid()) {
							if (key.isAcceptable()) {
								accept();
							} else {
								dispatch(key);
							}
						}
						iterator.remove();
					}

					selected.clear();
				}
			} catch (final Exception e) {
				LOGGER.error("ERROR::Accessing the selector", e);
			}

			if (m_running) {
				m_executor.execute(this);
			}
		}

		/**
		 * Dispatch SelectionKey events
		 * @param p_key
		 *            SelectionKey to handle
		 */
		private void dispatch(final SelectionKey p_key) {
			SelectionKeyHandler handler;
			final NIOConnection connection = (NIOConnection)p_key.attachment();

			if (connection != null) {
				if (connection.m_handler == null) {
					connection.m_handler = new SelectionKeyHandler(p_key);
				}

				handler = connection.m_handler;

				// dispatch event when the connection is known
				m_executor.execute(handler);
			} else {
				// handle unknown events by itself
				handler = new SelectionKeyHandler(p_key);
				handler.run();
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
			synchronized (m_changeRequests) {
				m_changeRequests.offer(p_request);
			}

			m_selector.wakeup();
		}

		/**
		 * Append the given CloseConnectionRequest to the Queue
		 * @param p_request
		 *            the CloseConnectionRequest
		 */
		protected void addCloseConnectionRequest(final CloseConnectionRequest p_request) {
			synchronized (m_closeRequests) {
				m_closeRequests.offer(p_request);
			}

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
			p_connection.m_channel.keyFor(m_selector).cancel();
			try {
				p_connection.m_channel.close();
			} catch (final IOException e) {
				LOGGER.error("ERROR::Could not close connection to " + p_connection.getDestination(), e);
			}

			fireConnectionClosed(p_connection);
		}

		/**
		 * Closes the Worker
		 */
		protected void close() {
			m_running = false;
			m_executor.shutdown();

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
	private class SelectionKeyHandler implements Runnable {

		private final SelectionKey m_key;
		private final ByteBuffer m_buffer;

		private final Object m_writeLock = new Object();

		private Boolean m_writing = false;
		private Boolean m_reading = false;

		/**
		 * Creates a new SelectionKeyHandler
		 * @param p_key
		 *            SelectionKey
		 */
		public SelectionKeyHandler(final SelectionKey p_key) {
			m_buffer = ByteBuffer.allocateDirect(INCOMING_BUFFER_SIZE);
			m_key = p_key;
		}

		@Override
		public void run() {
			final SelectionKey key = m_key;
			NIOConnection connection;

			if (key.isValid()) {
				try {
					connection = (NIOConnection)key.attachment();

					if (key.isReadable() && !m_reading) {
						synchronized (m_buffer) {
							m_reading = true;

							if (connection == null) {
								createConnection((SocketChannel)key.channel());
							} else {
								read(connection);
							}

							m_reading = false;
						}
					} else if (key.isWritable() && !m_writing) {
						synchronized (m_writeLock) {
							m_writing = true;

							write(connection);

							m_writing = false;
						}
					} else if (key.isConnectable()) {
						connect(connection);
					}
				} catch (final IOException e) {
					LOGGER.warn("WARN::Accessing the channel");
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
		private synchronized void createConnection(final SocketChannel p_channel) throws IOException {
			NIOConnection connection;
			ByteBuffer buffer;

			m_buffer.clear();

			if (p_channel.read(m_buffer) == -1) {
				p_channel.keyFor(m_worker.m_selector).cancel();
				p_channel.close();
			} else {
				m_buffer.flip();

				connection = new NIOConnection(InputHelper.readNodeID(m_buffer), p_channel);
				p_channel.register(m_worker.m_selector, SelectionKey.OP_READ, connection);

				fireConnectionCreated(connection);

				if (m_buffer.hasRemaining()) {
					buffer = ByteBuffer.allocate(m_buffer.remaining());
					buffer.put(m_buffer);
					buffer.flip();

					connection.addIncoming(buffer);
				}
			}
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

			m_buffer.clear();

			if (p_connection.m_channel.read(m_buffer) == -1) {
				m_worker.closeConnection(p_connection);
			} else {
				m_buffer.flip();

				ThroughputStatistic.getInstance().incomingExtern(m_buffer.remaining());

				buffer = ByteBuffer.allocate(m_buffer.remaining());
				buffer.put(m_buffer);
				buffer.flip();

				p_connection.addIncoming(buffer);
			}
		}

		/**
		 * Writes to the given connection
		 * @param p_connection
		 *            the connection
		 */
		private void write(final NIOConnection p_connection) {
			ByteBuffer[] buffers;

			buffers = p_connection.getOutgoingBytes(SEND_BYTES);
			try {
				while (buffers != null && buffers.length > 0) {
					int sum = 0;
					for (ByteBuffer buffer : buffers) {
						sum += buffer.remaining();
					}

					p_connection.m_channel.write(buffers);

					sum -= buffers[buffers.length - 1].remaining();
					ThroughputStatistic.getInstance().outgoingExtern(sum);

					if (buffers[buffers.length - 1].hasRemaining()) {
						buffers = null;
					} else {
						buffers = p_connection.getOutgoingBytes(SEND_BYTES);
					}
				}
			} catch (final IOException e) {
				LOGGER.error("ERROR::Could not write to channel (" + p_connection.getDestination() + ")", e);
				p_connection.close();
			}
		}

		/**
		 * Finishes the connection process for the given connection
		 * @param p_connection
		 *            the connection
		 */
		private synchronized void connect(final NIOConnection p_connection) {
			if (p_connection.m_channel.isConnectionPending()) {
				try {
					p_connection.m_channel.finishConnect();
					p_connection.connected();
				} catch (final IOException e) { /* ignore */}
			}
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
		public FlowControlMessage() {}

		/**
		 * Create a new Message for confirming received bytes.
		 * @param p_confirmedBytes
		 *            number of received bytes
		 */
		public FlowControlMessage(final int p_confirmedBytes) {
			super((short)0, TYPE, SUBTYPE);
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
