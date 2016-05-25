
package de.hhu.bsinfo.menet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents a network connection
 * @author Florian Klein 18.03.2012
 */
public class NIOConnection extends AbstractConnection {

	// Attributes
	private SocketChannel m_channel;
	private NIOSelector m_nioSelector;

	private int m_incomingBufferSize;
	private int m_outgoingBufferSize;
	private int m_numberOfBuffers;

	private ArrayDeque<ByteBuffer> m_incoming;
	private ArrayDeque<ByteBuffer> m_outgoing;
	private ReentrantLock m_connectionCondLock;
	private Condition m_connectionCond;

	private ReentrantLock m_incomingLock;
	private ReentrantLock m_outgoingAllLock;
	private ReentrantLock m_outgoingLock;

	private ChangeOperationsRequest m_writeOperation;

	// Constructors
	/**
	 * Creates an instance of NIOConnection (this node creates a new connection with another node)
	 * @param p_destination
	 *            the destination
	 * @param p_nodeMap
	 *            the node map
	 * @param p_taskExecutor
	 *            the task executer
	 * @param p_messageDirectory
	 *            the message directory
	 * @param p_lock
	 *            the ReentrantLock
	 * @param p_cond
	 *            the Condition
	 * @param p_nioSelector
	 *            the NIOSelector
	 * @param p_numberOfBuffers
	 *            the number of buffers to schedule
	 * @param p_incomingBufferSize
	 *            the size of incoming buffer
	 * @param p_outgoingBufferSize
	 *            the size of outgoing buffer
	 * @param p_flowControlWindowSize
	 *            the maximal number of ByteBuffer to schedule for sending/receiving
	 * @throws IOException
	 *             if the connection could not be created
	 */
	protected NIOConnection(final short p_destination, final NodeMap p_nodeMap, final TaskExecutor p_taskExecutor, final MessageDirectory p_messageDirectory,
			final ReentrantLock p_lock, final Condition p_cond, final NIOSelector p_nioSelector, final int p_numberOfBuffers, final int p_incomingBufferSize,
			final int p_outgoingBufferSize, final int p_flowControlWindowSize) throws IOException {
		super(p_destination, p_nodeMap, p_taskExecutor, p_messageDirectory, p_flowControlWindowSize);

		m_incomingBufferSize = p_incomingBufferSize;
		m_outgoingBufferSize = p_outgoingBufferSize;

		m_channel = SocketChannel.open();
		m_channel.configureBlocking(false);
		m_channel.socket().setSoTimeout(0);
		m_channel.socket().setTcpNoDelay(true);
		m_channel.socket().setReceiveBufferSize(m_incomingBufferSize);
		m_channel.socket().setSendBufferSize(m_outgoingBufferSize);

		m_channel.connect(super.getNodeMap().getAddress(p_destination));

		m_nioSelector = p_nioSelector;
		m_nioSelector.changeOperationInterestAsync(new ChangeOperationsRequest(this, SelectionKey.OP_CONNECT));

		m_incoming = new ArrayDeque<>();
		m_outgoing = new ArrayDeque<>();

		m_connectionCondLock = p_lock;
		m_connectionCond = p_cond;

		m_incomingLock = new ReentrantLock(false);
		m_outgoingAllLock = new ReentrantLock(false);
		m_outgoingLock = new ReentrantLock(false);

		m_numberOfBuffers = p_numberOfBuffers;

		m_writeOperation = new ChangeOperationsRequest(this, SelectionKey.OP_WRITE);
	}

	/**
	 * Creates an instance of NIOConnection (this node creates a new connection with another node)
	 * @param p_destination
	 *            the destination
	 * @param p_nodeMap
	 *            the node map
	 * @param p_taskExecutor
	 *            the task executer
	 * @param p_messageDirectory
	 *            the message directory
	 * @param p_channel
	 *            the socket channel
	 * @param p_nioSelector
	 *            the NIOSelector
	 * @param p_numberOfBuffers
	 *            the number of buffers to schedule
	 * @param p_incomingBufferSize
	 *            the size of incoming buffer
	 * @param p_outgoingBufferSize
	 *            the size of outgoing buffer
	 * @param p_flowControlWindowSize
	 *            the maximal number of ByteBuffer to schedule for sending/receiving
	 * @throws IOException
	 *             if the connection could not be created
	 */
	protected NIOConnection(final short p_destination, final NodeMap p_nodeMap, final TaskExecutor p_taskExecutor, final MessageDirectory p_messageDirectory,
			final SocketChannel p_channel, final NIOSelector p_nioSelector, final int p_numberOfBuffers,
			final int p_incomingBufferSize, final int p_outgoingBufferSize, final int p_flowControlWindowSize) throws IOException {
		super(p_destination, p_nodeMap, p_taskExecutor, p_messageDirectory, p_flowControlWindowSize);

		m_incomingBufferSize = p_incomingBufferSize;
		m_outgoingBufferSize = p_outgoingBufferSize;

		m_channel = p_channel;
		m_channel.configureBlocking(false);
		m_channel.socket().setSoTimeout(0);
		m_channel.socket().setTcpNoDelay(true);
		m_channel.socket().setReceiveBufferSize(m_incomingBufferSize);
		m_channel.socket().setSendBufferSize(m_outgoingBufferSize);

		m_nioSelector = p_nioSelector;

		m_incoming = new ArrayDeque<>();
		m_outgoing = new ArrayDeque<>();

		m_connectionCondLock = new ReentrantLock(false);
		m_connectionCond = m_connectionCondLock.newCondition();

		m_incomingLock = new ReentrantLock(false);
		m_outgoingAllLock = new ReentrantLock(false);
		m_outgoingLock = new ReentrantLock(false);

		m_numberOfBuffers = p_numberOfBuffers;

		m_writeOperation = new ChangeOperationsRequest(this, SelectionKey.OP_WRITE);
	}

	// Getter
	/**
	 * Returns the SocketChannel
	 * @return the SocketChannel
	 */
	public SocketChannel getChannel() {
		return m_channel;
	}

	// Methods
	/**
	 * Reads messages from the connection
	 * @return a AbstractMessage which was received
	 */
	@Override
	protected ByteBuffer doRead() {
		ByteBuffer ret;

		m_incomingLock.lock();
		ret = m_incoming.poll();
		m_incomingLock.unlock();

		return ret;
	}

	/**
	 * Append an incoming ByteBuffer to the Queue
	 * @param p_buffer
	 *            the ByteBuffer
	 */
	protected void addIncoming(final ByteBuffer p_buffer) {
		m_incomingLock.lock();
		// Avoid congestion by not allowing more than NUMBER_OF_BUFFERS buffers to be cached for reading
		while (m_incoming.size() > m_numberOfBuffers) {
			m_incomingLock.unlock();

			Thread.yield();

			m_incomingLock.lock();
		}

		m_incoming.offer(p_buffer);
		m_incomingLock.unlock();

		newData();
	}

	/**
	 * Writes data to the connection
	 * @param p_message
	 *            the AbstractMessage to send
	 */
	@Override
	protected void doWrite(final AbstractMessage p_message) throws NetworkException {
		if (p_message instanceof FlowControlMessage) {
			writeToChannelForced(p_message.getBuffer());
		} else {
			writeToChannel(p_message.getBuffer());
		}
	}

	@Override
	protected boolean dataLeftToWrite() {
		boolean ret;

		m_outgoingLock.lock();
		ret = !m_outgoing.isEmpty();
		m_outgoingLock.unlock();

		return ret;
	}

	@Override
	protected boolean isIncomingQueueFull() {
		boolean ret;

		m_incomingLock.lock();
		ret = m_incoming.size() >= m_numberOfBuffers;
		m_incomingLock.unlock();

		return ret;
	}

	@Override
	protected String getInputOutputQueueLength() {
		String ret = "";

		m_outgoingLock.lock();
		ret += "out: " + m_outgoing.size();
		m_outgoingLock.unlock();

		m_incomingLock.lock();
		ret += ", in: " + m_incoming.size();
		m_incomingLock.unlock();

		return ret;
	}

	/**
	 * Enqueue buffer to be written into the channel
	 * @param p_buffer
	 *            Buffer
	 */
	private void writeToChannel(final ByteBuffer p_buffer) {
		m_outgoingAllLock.lock();
		m_outgoingLock.lock();
		// Avoid congestion by not allowing more than NUMBER_OF_BUFFERS messages to be buffered for writing
		while (m_outgoing.size() > m_numberOfBuffers) {
			m_outgoingLock.unlock();
			m_outgoingAllLock.unlock();

			Thread.yield();

			m_outgoingAllLock.lock();
			m_outgoingLock.lock();
		}

		m_outgoing.offer(p_buffer);
		m_outgoingLock.unlock();

		// Change operation (read <-> write) and/or connection
		m_nioSelector.changeOperationInterestAsync(m_writeOperation);
		m_outgoingAllLock.unlock();
	}

	/**
	 * Enqueue buffer to be written into the channel
	 * @param p_buffer
	 *            Buffer
	 */
	private void writeToChannelForced(final ByteBuffer p_buffer) {
		m_outgoingAllLock.lock();
		m_outgoingLock.lock();
		m_outgoing.offer(p_buffer);
		m_outgoingLock.unlock();

		// Change operation (read <-> write) and/or connection
		m_nioSelector.changeOperationInterestAsync(m_writeOperation);
		m_outgoingAllLock.unlock();
	}

	/**
	 * Prepend buffer to be written into the channel. Called if buffer could not be written completely.
	 * @param p_buffer
	 *            Buffer
	 */
	void addBuffer(final ByteBuffer p_buffer) {
		m_outgoingLock.lock();
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
		int length = 0;
		ByteBuffer buffer;
		ByteBuffer ret = null;

		while (true) {
			m_outgoingLock.lock();
			if (m_outgoing.isEmpty()) {
				m_outgoingLock.unlock();
				break;
			}
			buffer = m_outgoing.poll();
			m_outgoingLock.unlock();

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
		m_nioSelector.closeConnectionAsync(this);
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
			m_channel.socket().setReceiveBufferSize(m_incomingBufferSize);
			m_channel.socket().setSendBufferSize(m_outgoingBufferSize);
		} catch (final IOException e) {
			m_connectionCondLock.unlock();
			throw e;
		}

		temp = ByteBuffer.allocate(2);
		temp.putShort(super.getNodeMap().getOwnNodeID());
		temp.flip();

		writeToChannel(temp);

		setConnected(true);

		m_connectionCond.signalAll();
		m_connectionCondLock.unlock();
	}

	@Override
	public String toString() {
		String ret;

		ret = super.toString();
		try {
			ret += ", address: " + m_channel.getRemoteAddress() + "]\n";
		} catch (final IOException e) {
			e.printStackTrace();
		}

		return ret;
	}

}
